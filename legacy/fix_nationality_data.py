"""
Script para limpiar nacionalidades malformadas en MongoDB
"""
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import re

load_dotenv()

MONGO_URI = os.environ.get("MONGODB_URI")
client = MongoClient(MONGO_URI)
db = client["ufc_picks"]
bouts_col = db.bouts

print("Limpiando nacionalidades...")

updated_count = 0

for bout in bouts_col.find():
    needs_update = False
    update_fields = {}

    fighters = bout.get("fighters", {})

    for corner in ["red", "blue"]:
        fighter = fighters.get(corner, {})
        if not fighter:
            continue

        nationality = fighter.get("nationality")

        if nationality and isinstance(nationality, str):
            # Limpiar nacionalidad
            # 1. Quitar saltos de línea múltiples
            cleaned = re.sub(r'\s+', ' ', nationality)
            # 2. Quitar espacios al inicio y final
            cleaned = cleaned.strip()
            # 3. Si hay duplicados como "United States United States", tomar el primero
            words = cleaned.split()
            # Buscar patrones comunes de países
            known_countries = [
                "United States", "Brazil", "Russia", "Canada", "Mexico",
                "United Kingdom", "Ireland", "Poland", "Japan", "China",
                "Afghanistan", "Iraq", "Ukraine", "Kazakhstan", "Tajikistan"
            ]

            # Intentar encontrar un país conocido
            final_nationality = None
            for country in known_countries:
                if country.lower() in cleaned.lower():
                    final_nationality = country
                    break

            # Si no encontramos un país conocido, usar la primera palabra capitalizada
            if not final_nationality:
                # Tomar primeras 1-2 palabras que parezcan nombre de país
                if len(words) >= 2 and words[0][0].isupper() and words[1][0].isupper():
                    final_nationality = f"{words[0]} {words[1]}"
                elif len(words) >= 1 and words[0][0].isupper():
                    final_nationality = words[0]
                else:
                    final_nationality = "Unknown"

            # Si cambió, actualizar
            if final_nationality != nationality:
                update_fields[f"fighters.{corner}.nationality"] = final_nationality
                needs_update = True
                print(f"  {bout.get('_id')} - {corner}: '{nationality[:50]}...' -> '{final_nationality}'")

    if needs_update and update_fields:
        bouts_col.update_one(
            {"_id": bout["_id"]},
            {"$set": update_fields}
        )
        updated_count += 1

print(f"\nTotal peleas actualizadas: {updated_count}")
print("Listo!")
