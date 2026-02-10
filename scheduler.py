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
for event in events.find({"status": "upcoming"}):
    start_utc = parse_et_time(event["event_date"], event["start_time_et"])

    # Diferentes ventanas de tiempo según el tipo de evento
    if event["event_type"] == "numbered":
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

            # Ejecutar scraper
            subprocess.run([
                "scrapy", "crawl", "ufc",
                "-a", f"EVENT_ID={event['_id']}",
                "-a", "MODE=results",
                "-o", "raw.jsonl"
            ], check=True)

            # Ingerir en MongoDB
            print(f"Ingiriendo resultados...")
            subprocess.run(["python", "ingest.py"], check=True)

            # Marcar como completada
            events.update_one(
                {"_id": event["_id"]},
                {"$push": {"scrape_windows_done": idx}}
            )

            break  # Una corrida por ejecución
