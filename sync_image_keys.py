"""
Script para sincronizar image_keys desde S3 a MongoDB

Problema: Imágenes existen en S3 pero MongoDB no tiene el image_key seteado
Solución: Listar S3, parsear tapology_ids de los nombres de archivo, actualizar MongoDB
"""

import os
import re
from pymongo import MongoClient
from dotenv import load_dotenv
import boto3

load_dotenv()

# MongoDB
MONGO_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGO_URI)
db = client["ufc_picks"]

# S3
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def sync_image_keys():
    """Sincronizar image_keys desde S3 a MongoDB"""

    print("🔍 Listando imágenes en S3...")

    # Listar todos los objetos en fighters/
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=AWS_S3_BUCKET, Prefix='fighters/')

    updated_count = 0
    not_found_count = 0

    for page in pages:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']  # e.g., "fighters/33428.png"

            # Extraer tapology_id del nombre del archivo (numérico o slug)
            match = re.search(r'fighters/([^/]+)\.(jpg|png|webp)', key)
            if not match:
                continue

            tapology_id = match.group(1)

            print(f"📝 Processing {key} (tapology_id: {tapology_id})")

            # Actualizar MongoDB
            red_result = db.bouts.update_many(
                {"fighters.red.tapology_id": tapology_id},
                {"$set": {"fighters.red.image_key": key}}
            )

            blue_result = db.bouts.update_many(
                {"fighters.blue.tapology_id": tapology_id},
                {"$set": {"fighters.blue.image_key": key}}
            )

            total = red_result.modified_count + blue_result.modified_count

            if total > 0:
                print(f"  ✅ Updated {total} bouts")
                updated_count += total
            else:
                print(f"  ⚠️  No bouts found for tapology_id {tapology_id}")
                not_found_count += 1

    print(f"\n{'='*50}")
    print(f"SYNC COMPLETE")
    print(f"{'='*50}")
    print(f"✅ Bouts updated: {updated_count}")
    print(f"⚠️  Fighters not found in DB: {not_found_count}")

if __name__ == "__main__":
    sync_image_keys()
