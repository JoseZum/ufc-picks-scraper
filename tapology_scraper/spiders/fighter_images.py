"""
Spider de Imágenes de Peleadores UFC - Sube directamente a S3

Este spider se enfoca exclusivamente en descargar y almacenar imágenes de peleadores.
A diferencia del spider de datos (ufc_fighters.py), este maneja solo imágenes.

Flujo del spider:
1. Consulta MongoDB para encontrar peleadores sin imagen (image_key = None)
2. Visita el perfil de cada peleador en Tapology
3. Extrae la URL de la imagen del peleador (headshot o profile image)
4. Descarga la imagen en memoria (NO la guarda en disco local)
5. Sube directamente a S3 usando el servicio centralizado
6. Guarda en MongoDB solo el image_key (NO la URL completa)

Naming convention en S3:
- fighters/{fighter_id}.jpg
- Ejemplo: fighters/123456.jpg

Usage:
    scrapy crawl fighter_images                        # Todos los peleadores sin imagen
    scrapy crawl fighter_images -a EVENT_ID=135755     # Solo un evento específico
    scrapy crawl fighter_images -a LIMIT=50            # Limitar cantidad
    scrapy crawl fighter_images -a FIGHTER_ID=123456   # Un peleador específico
"""

import scrapy
import os
import re
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Optional


class FighterImagesSpider(scrapy.Spider):
    name = "fighter_images"
    allowed_domains = ["tapology.com", "images.tapology.com"]

    custom_settings = {
        # Delay entre requests - ser respetuoso con Tapology
        "DOWNLOAD_DELAY": 1.5,
        # Un request a la vez por dominio - evita ban
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        # Ignorar robots.txt (necesario para acceder a páginas de perfil)
        "ROBOTSTXT_OBEY": False,
        "FEED_EXPORT_ENCODING": "utf-8",
        # Pipeline personalizado para subir a S3
        "ITEM_PIPELINES": {
            'tapology_scraper.spiders.fighter_images.FighterImagesPipeline': 300,
        },
        # Headers para simular navegador real y evitar detección como bot
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://www.tapology.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    }

    def __init__(self, EVENT_ID=None, LIMIT=None, FIGHTER_ID=None, *args, **kwargs):
        """
        Inicializar spider con parámetros opcionales

        Args:
            EVENT_ID: Filtrar solo peleadores de un evento específico
            LIMIT: Limitar cantidad de peleadores a procesar
            FIGHTER_ID: Procesar solo un peleador específico
        """
        super(FighterImagesSpider, self).__init__(*args, **kwargs)
        self.target_event_id = EVENT_ID
        self.target_fighter_id = FIGHTER_ID
        self.limit = int(LIMIT) if LIMIT else None

        # Conectar a MongoDB
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en variables de entorno")

        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks

        # Log de configuración
        self.logger.info(f"Fighter Images Spider initialized")
        if self.target_event_id:
            self.logger.info(f"📌 Filtering by event: {self.target_event_id}")
        if self.target_fighter_id:
            self.logger.info(f"📌 Processing single fighter: {self.target_fighter_id}")
        if self.limit:
            self.logger.info(f"📌 Limit: {self.limit} fighters")

    async def start(self):
        """
        Punto de entrada asíncrono del spider

        Carga los peleadores desde MongoDB y genera requests
        """
        async for req in self.load_fighters_from_mongo():
            yield req

    async def load_fighters_from_mongo(self):
        """
        Cargar peleadores que necesitan imágenes desde MongoDB

        Busca en la colección 'bouts' todos los fighters que:
        - No tienen image_key (campo no existe o es None)
        - Tienen tapology_id y tapology_url válidos

        ¿Por qué buscamos en 'bouts' y no en una colección 'fighters'?
        Porque actualmente los datos de fighters están embebidos en bouts.
        Esto puede cambiar en el futuro si creamos una colección separada.

        Yields:
            scrapy.Request para cada peleador único sin imagen
        """
        # Construir query para encontrar fighters sin imagen
        # Buscamos donde image_key no existe o es None en red o blue corner
        query = {
            "$or": [
                {"fighters.red.image_key": None},
                {"fighters.red.image_key": {"$exists": False}},
                {"fighters.blue.image_key": None},
                {"fighters.blue.image_key": {"$exists": False}},
            ]
        }

        # Aplicar filtros opcionales
        if self.target_event_id:
            query["event_id"] = int(self.target_event_id)

        try:
            # Obtener todos los bouts que coincidan
            bouts = await self.db.bouts.find(query).to_list(length=None)
            self.logger.info(f"📊 Found {len(bouts)} bouts with fighters missing images")

            # Trackear fighters únicos para evitar duplicados
            # Usamos tapology_id como identificador único
            seen_fighter_ids = set()
            fighter_count = 0

            for bout in bouts:
                # Si llegamos al límite, parar
                if self.limit and fighter_count >= self.limit:
                    break

                bout_id = bout.get("id") or bout.get("_id")
                fighters = bout.get("fighters", {})

                # Procesar ambos corners (red y blue)
                for corner in ["red", "blue"]:
                    if self.limit and fighter_count >= self.limit:
                        break

                    fighter = fighters.get(corner, {})
                    tapology_id = fighter.get("tapology_id")
                    tapology_url = fighter.get("tapology_url")
                    fighter_name = fighter.get("fighter_name", "Unknown")

                    # Validar que tenga los datos necesarios
                    if not tapology_id or not tapology_url:
                        self.logger.warning(
                            f"⚠️  Bout {bout_id} {corner} fighter missing tapology data"
                        )
                        continue

                    # Si especificamos FIGHTER_ID, filtrar
                    if self.target_fighter_id and tapology_id != self.target_fighter_id:
                        continue

                    # Evitar duplicados
                    if tapology_id in seen_fighter_ids:
                        continue

                    # Verificar si realmente necesita imagen
                    image_key = fighter.get("image_key")
                    if image_key:
                        # Ya tiene imagen, skip
                        continue

                    # Agregar a la lista de vistos
                    seen_fighter_ids.add(tapology_id)
                    fighter_count += 1

                    self.logger.info(
                        f"🎯 Queuing fighter: {fighter_name} (ID: {tapology_id})"
                    )

                    # Generar request para visitar el perfil
                    yield scrapy.Request(
                        url=tapology_url,
                        callback=self.parse_fighter_image,
                        meta={
                            "tapology_id": tapology_id,
                            "fighter_name": fighter_name,
                        },
                        errback=self.handle_error,
                        dont_filter=True
                    )

            self.logger.info(f"✅ Total unique fighters to process: {fighter_count}")

        except Exception as e:
            self.logger.error(f"❌ Error loading fighters from MongoDB: {e}")

    def parse_fighter_image(self, response):
        """
        Extraer la URL de la imagen del perfil del peleador

        Tapology muestra imágenes de peleadores en diferentes formatos:
        - Headshot images: fotos de cara/perfil (preferidas)
        - Letterbox images: fotos promocionales/artísticas
        - Profile images: fotos de perfil genéricas

        Estrategia de selección:
        1. Buscar primero headshot_images (mejor calidad para perfiles)
        2. Si no hay, buscar letterbox_images
        3. Si no hay, buscar cualquier imagen de perfil

        ¿Por qué este orden?
        - headshot_images son consistentes en tamaño y formato
        - letterbox_images a veces tienen fondos o texto superpuesto
        - Es mejor tener una imagen consistente que la más "artística"

        Args:
            response: Scrapy response del perfil del peleador

        Yields:
            Dict con tapology_id, fighter_name, e image_url para el pipeline
        """
        tapology_id = response.meta["tapology_id"]
        fighter_name = response.meta["fighter_name"]

        self.logger.info(f"🔍 Parsing image for: {fighter_name}")

        # Estrategia 1: Buscar headshot images
        # Selector: img[src*="headshot_images"]
        # Estas son las imágenes de cara/perfil de alta calidad
        headshot_imgs = response.css('img[src*="headshot_images"]::attr(src)').getall()

        if headshot_imgs:
            # Tomar la primera headshot encontrada
            image_url = headshot_imgs[0]
            self.logger.info(f"✅ Found headshot image for {fighter_name}")
            yield self._create_image_item(tapology_id, fighter_name, image_url, "headshot")
            return

        # Estrategia 2: Buscar letterbox images
        # Estas son imágenes promocionales, segunda opción
        letterbox_imgs = response.css('img[src*="letterbox_images"]::attr(src)').getall()

        if letterbox_imgs:
            image_url = letterbox_imgs[0]
            self.logger.info(f"✅ Found letterbox image for {fighter_name}")
            yield self._create_image_item(tapology_id, fighter_name, image_url, "letterbox")
            return

        # Estrategia 3: Buscar cualquier imagen de perfil
        # Selector más amplio como fallback
        profile_imgs = response.css(
            'img[src*="profile"], img.fighter-image::attr(src), '
            'div.fighter-photo img::attr(src)'
        ).getall()

        if profile_imgs:
            image_url = profile_imgs[0]
            self.logger.info(f"✅ Found profile image for {fighter_name}")
            yield self._create_image_item(tapology_id, fighter_name, image_url, "profile")
            return

        # No se encontró ninguna imagen
        self.logger.warning(f"⚠️  No image found for {fighter_name} (ID: {tapology_id})")

    def _create_image_item(
        self,
        tapology_id: str,
        fighter_name: str,
        image_url: str,
        image_type: str
    ) -> dict:
        """
        Crear item para procesar en el pipeline

        Normaliza la URL de la imagen y prepara el item con toda la metadata
        necesaria para el pipeline.

        Args:
            tapology_id: ID único del peleador en Tapology
            fighter_name: Nombre del peleador (para logging)
            image_url: URL de la imagen (puede ser relativa o absoluta)
            image_type: Tipo de imagen encontrada (headshot/letterbox/profile)

        Returns:
            Dict con los datos necesarios para el pipeline
        """
        # Normalizar URL: convertir a absoluta si es relativa
        if image_url.startswith('//'):
            # URL sin protocolo (ej: //images.tapology.com/...)
            image_url = f"https:{image_url}"
        elif image_url.startswith('/'):
            # URL relativa (ej: /headshot_images/...)
            image_url = f"https://images.tapology.com{image_url}"
        elif not image_url.startswith('http'):
            # URL relativa sin / inicial
            image_url = f"https://images.tapology.com/{image_url}"

        return {
            "type": "fighter_image",
            "tapology_id": tapology_id,
            "fighter_name": fighter_name,
            "image_url": image_url,
            "image_type": image_type,
        }

    def handle_error(self, failure):
        """
        Manejar errores de requests HTTP

        Se ejecuta cuando falla un request (timeout, 404, 500, etc).
        Loguea el error para debugging pero no frena el spider.

        Args:
            failure: Scrapy Failure object con información del error
        """
        request = failure.request
        tapology_id = request.meta.get("tapology_id", "unknown")
        fighter_name = request.meta.get("fighter_name", "unknown")

        self.logger.error(f"❌ Request failed for {fighter_name} (ID: {tapology_id})")
        self.logger.error(f"   URL: {request.url}")
        self.logger.error(f"   Reason: {failure.value}")

    async def close(self, reason):
        """
        Cleanup al cerrar el spider

        Args:
            reason: Razón de cierre del spider (finished/cancelled/etc)
        """
        self.mongo_client.close()
        self.logger.info(f"🏁 Spider closed: {reason}")


class FighterImagesPipeline:
    """
    Pipeline para descargar imágenes y subirlas a S3

    Este pipeline procesa cada item generado por el spider:
    1. Descarga la imagen desde Tapology (en memoria, NO en disco)
    2. Sube la imagen a S3 usando el servicio centralizado
    3. Actualiza MongoDB con el image_key (NO la URL)

    ¿Por qué guardar solo image_key y no URL?
    - Las URLs de CloudFront pueden cambiar (si cambiamos de CDN)
    - El backend genera la URL completa desde el image_key
    - Menor acoplamiento entre datos y infraestructura

    Estructura en MongoDB después de este pipeline:
    {
        "fighters": {
            "red": {
                "tapology_id": "123456",
                "fighter_name": "John Doe",
                "image_key": "fighters/123456.jpg",  // ← Lo que guardamos
                ...
            }
        }
    }

    El backend luego usa image_key para construir:
    https://d6huioh3922nf.cloudfront.net/fighters/123456.jpg
    """

    def __init__(self):
        """
        Inicializar pipeline

        Conecta a MongoDB y configura el servicio S3.
        Si S3 no está configurado, el pipeline falla early (fail-fast).
        """
        # Conectar a MongoDB
        mongo_uri = os.getenv("MONGODB_URI")
        if not mongo_uri:
            raise RuntimeError("MONGODB_URI no está definida en variables de entorno")

        self.mongo_client = AsyncIOMotorClient(mongo_uri)
        self.db = self.mongo_client.ufc_picks

        # Validar que las variables de S3 estén configuradas
        # Esto falla temprano si algo falta, mejor que fallar a mitad de scraping
        required_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_S3_BUCKET"
        ]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise RuntimeError(
                f"Variables de S3 faltantes: {', '.join(missing_vars)}. "
                "Este spider requiere configuración completa de S3."
            )

        # Importar y configurar servicio S3
        # La importación está aquí en vez de arriba porque solo se necesita
        # si efectivamente vamos a procesar items
        try:
            import sys
            import pathlib

            # Agregar el path del backend al sys.path para importar el servicio
            backend_path = pathlib.Path(__file__).parent.parent.parent.parent / "backend"
            sys.path.insert(0, str(backend_path))

            from app.services.s3_service import get_s3_service

            self.s3_service = get_s3_service()

            # Verificar que estemos en modo escritura, no cache-only
            if self.s3_service.is_read_only:
                raise RuntimeError(
                    "IMAGE_SOURCE_MODE está en 'cache' (solo lectura). "
                    "Cambia a 's3' para permitir subida de imágenes."
                )

        except ImportError as e:
            raise RuntimeError(
                f"No se pudo importar el servicio S3 del backend: {e}. "
                "Asegúrate de que el backend esté en la estructura esperada."
            )

    async def process_item(self, item, spider):
        """
        Procesar cada item: descargar imagen y subir a S3

        Flujo:
        1. Validar que sea un item de tipo fighter_image
        2. Descargar la imagen desde Tapology (httpx, en memoria)
        3. Generar S3 key usando el servicio (fighters/{fighter_id}.jpg)
        4. Subir a S3 con metadata
        5. Actualizar MongoDB con el image_key en todos los bouts del fighter

        Args:
            item: Dict con tapology_id, fighter_name, image_url, etc
            spider: Referencia al spider (para logging)

        Returns:
            El mismo item (Scrapy pipeline protocol)
        """
        # Validar tipo de item
        if item.get("type") != "fighter_image":
            return item

        tapology_id = item.get("tapology_id")
        fighter_name = item.get("fighter_name")
        image_url = item.get("image_url")
        image_type = item.get("image_type", "unknown")

        if not tapology_id or not image_url:
            spider.logger.warning(f"⚠️  Item inválido, faltan campos: {item}")
            return item

        spider.logger.info(f"📥 Processing image for {fighter_name} (type: {image_type})")

        try:
            # Paso 1: Descargar la imagen en memoria
            spider.logger.info(f"⬇️  Downloading image from Tapology...")

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    image_url,
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Referer": "https://www.tapology.com/",
                    },
                    follow_redirects=True
                )

                if response.status_code != 200:
                    spider.logger.error(
                        f"❌ Failed to download image: HTTP {response.status_code}"
                    )
                    return item

                # Imagen descargada exitosamente en memoria
                image_bytes = response.content
                content_type = response.headers.get("content-type", "image/jpeg")

            spider.logger.info(
                f"✅ Downloaded {len(image_bytes)} bytes (type: {content_type})"
            )

            # Paso 2: Generar S3 key
            # El servicio S3 maneja el naming convention: fighters/{fighter_id}.jpg
            file_ext = self._get_extension_from_content_type(content_type)
            s3_key = self.s3_service.generate_fighter_image_key(tapology_id, file_ext)

            spider.logger.info(f"📤 Uploading to S3: {s3_key}")

            # Paso 3: Subir a S3
            await self.s3_service.upload_image(
                s3_key=s3_key,
                image_data=image_bytes,
                content_type=content_type,
                metadata={
                    "tapology_id": tapology_id,
                    "fighter_name": fighter_name,
                    "image_type": image_type,
                    "source": "tapology"
                }
            )

            spider.logger.info(f"✅ Uploaded to S3 successfully")

            # Paso 4: Actualizar MongoDB
            # Actualizar TODOS los bouts donde este fighter aparezca (red o blue)
            # Guardamos solo el image_key, NO la URL completa

            # Update para red corner
            red_result = await self.db.bouts.update_many(
                {"fighters.red.tapology_id": tapology_id},
                {"$set": {"fighters.red.image_key": s3_key}}
            )

            # Update para blue corner
            blue_result = await self.db.bouts.update_many(
                {"fighters.blue.tapology_id": tapology_id},
                {"$set": {"fighters.blue.image_key": s3_key}}
            )

            total_updated = red_result.modified_count + blue_result.modified_count

            if total_updated > 0:
                spider.logger.info(
                    f"💾 Updated {total_updated} bouts with image_key for {fighter_name}"
                )
            else:
                spider.logger.warning(
                    f"⚠️  No bouts updated for {fighter_name} (tapology_id: {tapology_id})"
                )

        except Exception as e:
            spider.logger.error(
                f"❌ Error processing image for {fighter_name}: {e}"
            )

        return item

    def _get_extension_from_content_type(self, content_type: str) -> str:
        """
        Inferir extensión de archivo desde Content-Type HTTP

        Args:
            content_type: MIME type (ej: "image/jpeg", "image/png")

        Returns:
            Extensión sin punto (ej: "jpg", "png")
        """
        content_type_lower = content_type.lower()

        if "jpeg" in content_type_lower or "jpg" in content_type_lower:
            return "jpg"
        elif "png" in content_type_lower:
            return "png"
        elif "gif" in content_type_lower:
            return "gif"
        elif "webp" in content_type_lower:
            return "webp"

        # Default a JPG (más común en Tapology)
        return "jpg"

    def close_spider(self, spider):
        """
        Cleanup al cerrar el spider

        Args:
            spider: Referencia al spider que está cerrando
        """
        self.mongo_client.close()
        spider.logger.info("💾 MongoDB connection closed")
