"""
Scheduler de scraping automático.

Ejecuta el scraper en ventanas de tiempo estratégicas después de que
comienzan los eventos, para capturar resultados a medida que terminen las pele as.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import subprocess
import pytz

MONGO_URI = os.environ.get("MONGODB_URI", "")
if not MONGO_URI:
    print("ERROR: MONGODB_URI no configurada")
    sys.exit(1)

client = MongoClient(MONGO_URI)
db = client.ufc_picks

events = db.events

NOW = datetime.now(timezone.utc)

def parse_et_time(event_date_dt, time_str):
    """Convierte hora en ET a UTC, considerando DST automáticamente."""
    et_tz = pytz.timezone("America/New_York")

    # Extraer la fecha
    if isinstance(event_date_dt, datetime):
        event_date = event_date_dt.date()
    else:
        event_date = event_date_dt

    h, m = map(int, time_str.split(":"))

    # Crear datetime sin zona horaria
    naive_dt = datetime.combine(event_date, datetime.min.time()).replace(hour=h, minute=m)

    # Localizar a ET (DST automático)
    et_dt = et_tz.localize(naive_dt)

    # Convertir a UTC
    return et_dt.astimezone(pytz.utc).replace(tzinfo=None)

# Procesar eventos próximos
for event in events.find({"status": "scheduled"}):
    event_date = event.get("date") or event.get("event_date")
    start_time = event.get("start_time_et")
    if not event_date or not start_time:
        print(f"Skipping event {event.get('id')}: missing date or ET start time")
        continue

    start_utc = parse_et_time(event_date, start_time)

    # Diferentes ventanas de tiempo según el tipo de evento
    if event.get("event_type") == "numbered":
        # UFC numerados: 3 ventanas en 8+ horas
        windows = [
            start_utc + timedelta(hours=2, minutes=30),
            start_utc + timedelta(hours=5),
            start_utc + timedelta(hours=8, minutes=15),
        ]
    else:
        # Fight Night: 2 ventanas más cortas
        windows = [
            start_utc + timedelta(hours=3, minutes=30),
            start_utc + timedelta(hours=4),
        ]

    already = event.get("scrape_windows_done", [])

    for idx, window in enumerate(windows):
        if idx in already:
            continue

        # Si es hora de este scrape
        if NOW >= window:
            print(f"Scraping evento {event['_id']} (ventana {idx})")

            # Limpiar raw.jsonl antes de cada scrape para evitar data vieja
            raw_file = os.path.join(os.path.dirname(__file__), ".runtime-results.jsonl")

            # Ejecutar scraper
            event_url = event.get("tapology_url")
            if not event_url:
                print(f"Skipping event {event.get('id')}: no Tapology URL is available for result ingestion")
                continue
            scrapy_command = [
                "scrapy", "crawl", "ufc",
                "-a", f"EVENT_ID={event['id']}",
                "-a", "MODE=results",
            ]
            if event_url:
                scrapy_command.extend(["-a", f"EVENT_URL={event_url}"])
            # -O overwrites the temporary feed; -o appends and can replay stale data.
            scrapy_command.extend(["-O", raw_file])

            subprocess.run(scrapy_command, check=True)

            subprocess.run(
                [
                    sys.executable,
                    os.path.join(os.path.dirname(__file__), "validate_feed.py"),
                    "--raw-file", raw_file,
                    "--min-events", "1",
                    "--min-bouts", "1",
                ],
                check=True,
            )

            # Ingerir en MongoDB
            print(f"Ingiriendo resultados...")
            subprocess.run(
                [sys.executable, os.path.join(os.path.dirname(__file__), "ingest.py"), "--raw-file", raw_file],
                check=True,
            )

            # Marcar como completada
            events.update_one(
                {"_id": event["_id"]},
                {"$push": {"scrape_windows_done": idx}}
            )

            break  # Una corrida por ejecución
