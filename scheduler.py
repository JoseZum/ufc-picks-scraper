import os
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import subprocess
import pytz

MONGO_URI = os.environ["MONGODB_URI"]
client = MongoClient(MONGO_URI)
db = client.ufc_picks

events = db.events

NOW = datetime.now(timezone.utc)

def parse_et_time(event_date_dt, time_str):
    """Parsea hora en ET a UTC, considerando horario de verano (DST)."""
    et_tz = pytz.timezone("America/New_York")
    
    # Extraer solo la parte de fecha
    if isinstance(event_date_dt, datetime):
        event_date = event_date_dt.date()
    else:
        event_date = event_date_dt
    
    h, m = map(int, time_str.split(":"))
    
    # Crear datetime 'naive' (sin zona horaria)
    naive_dt = datetime.combine(event_date, datetime.min.time()).replace(hour=h, minute=m)
    
    # Localizar a ET (maneja DST automáticamente)
    et_dt = et_tz.localize(naive_dt)
    
    # Convertir a UTC
    return et_dt.astimezone(pytz.utc).replace(tzinfo=None)

for event in events.find({"status": "upcoming"}):
    start_utc = parse_et_time(event["event_date"], event["start_time_et"])

    if event["event_type"] == "numbered":
        windows = [
            start_utc + timedelta(hours=2, minutes=30),
            start_utc + timedelta(hours=5),
            start_utc + timedelta(hours=8, minutes=15),
        ]
    else:
        windows = [
            start_utc + timedelta(hours=3, minutes=30),
            start_utc + timedelta(hours=4),
        ]

    already = event.get("scrape_windows_done", [])

    for idx, window in enumerate(windows):
        if idx in already:
            continue

        if NOW >= window:
            print(f"Scraping event {event['_id']} (window {idx})")

            # Ejecutar scraping de resultados
            subprocess.run([
                "scrapy", "crawl", "ufc",
                "-a", f"EVENT_ID={event['_id']}",
                "-a", "MODE=results",
                "-o", "raw.jsonl"
            ], check=True)

            # Ingerir los resultados en MongoDB
            print(f"Ingerir resultados en MongoDB...")
            subprocess.run(["python", "ingest.py"], check=True)

            # Marcar la ventana como completada
            events.update_one(
                {"_id": event["_id"]},
                {"$push": {"scrape_windows_done": idx}}
            )

            break  # solo una corrida por ejecución
