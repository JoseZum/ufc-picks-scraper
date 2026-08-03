"""
Script para arreglar el campo ranking de int a dict en MongoDB
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.environ.get("MONGODB_URI")
client = MongoClient(MONGO_URI)
db = client["ufc_picks"]
bouts_col = db.bouts

print("Arreglando campo ranking...")

# Buscar todas las peleas con ranking como int
updated_count = 0

for bout in bouts_col.find():
    needs_update = False
    update_fields = {}

    fighters = bout.get("fighters", {})

    for corner in ["red", "blue"]:
        fighter = fighters.get(corner, {})
        if not fighter:
            continue

        ranking = fighter.get("ranking")

        # Si ranking es un int, convertirlo a dict
        if isinstance(ranking, int):
            # Buscar si hay ufc_ranking
            ufc_ranking = fighter.get("ufc_ranking")
            if ufc_ranking and isinstance(ufc_ranking, dict):
                # Usar ufc_ranking
                update_fields[f"fighters.{corner}.ranking"] = ufc_ranking
            else:
                # Crear dict con solo position
                update_fields[f"fighters.{corner}.ranking"] = {
                    "position": ranking,
                    "division": fighter.get("ranking_division", "Unknown")
                }
            needs_update = True

        # También arreglar gym si está como string
        gym = fighter.get("gym")
        if isinstance(gym, str):
            update_fields[f"fighters.{corner}.gym"] = {
                "primary": gym,
                "other": []
            }
            needs_update = True

    if needs_update and update_fields:
        bouts_col.update_one(
            {"_id": bout["_id"]},
            {"$set": update_fields}
        )
        updated_count += 1
        if updated_count % 10 == 0:
            print(f"  Actualizadas {updated_count} peleas...")

print(f"\nTotal peleas actualizadas: {updated_count}")
print("Listo!")
