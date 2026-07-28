"""
Servicio S3 simplificado para el scraper

Versión autónoma del servicio S3 para subir imágenes a AWS S3.
No depende del backend, obtiene configuración directamente de variables de entorno.
"""

import os
from io import BytesIO
from typing import Optional
import unicodedata


class S3ServiceError(Exception):
    """Error base para excepciones del servicio S3"""
    pass


class S3NotConfiguredError(S3ServiceError):
    """Se intentó usar S3 sin configurar las credenciales necesarias"""
    pass


class S3WriteNotAllowedError(S3ServiceError):
    """Se intentó escribir en S3 estando en modo cache (solo lectura)"""
    pass


class S3Service:
    """
    Servicio simplificado para subir imágenes a S3

    Solo incluye las funcionalidades necesarias para el spider de imágenes:
    - Generar keys para imágenes de fighters
    - Subir imágenes a S3
    """

    def __init__(self):
        self._s3_client = None

        # Cargar configuración desde variables de entorno
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.getenv("AWS_REGION", "us-east-1")
        self.aws_s3_bucket = os.getenv("AWS_S3_BUCKET")
        self.image_source_mode = os.getenv("IMAGE_SOURCE_MODE", "s3")

        # Validar que el modo de origen sea válido
        if self.image_source_mode not in ["s3", "cache"]:
            raise ValueError(
                f"IMAGE_SOURCE_MODE inválido: {self.image_source_mode}. "
                "Debe ser 's3' o 'cache'"
            )

    @property
    def s3_client(self):
        """
        Cliente de S3 lazy-loaded

        Solo se inicializa cuando realmente se necesita, y se cachea para
        reutilizar la misma conexión.
        """
        if self._s3_client is None:
            if not all([
                self.aws_access_key_id,
                self.aws_secret_access_key,
                self.aws_s3_bucket
            ]):
                raise S3NotConfiguredError(
                    "S3 no está configurado. Faltan: AWS_ACCESS_KEY_ID, "
                    "AWS_SECRET_ACCESS_KEY o AWS_S3_BUCKET"
                )

            try:
                import boto3
                self._s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.aws_region
                )
            except ImportError:
                raise S3NotConfiguredError(
                    "boto3 no está instalado. Instalar con: pip install boto3"
                )

        return self._s3_client

    @property
    def is_read_only(self) -> bool:
        """
        Indica si estamos en modo solo lectura (cache)
        """
        return self.image_source_mode == "cache"

    def generate_fighter_image_key(self, fighter_id: str, file_ext: str = "jpg") -> str:
        """
        Genera la key S3 para la imagen de un peleador

        Convención de nombres:
        - fighters/{fighter_id}.jpg
        - Ejemplo: fighters/123456.jpg

        Args:
            fighter_id: ID del peleador (puede ser string o int)
            file_ext: Extensión del archivo (default: jpg)

        Returns:
            Key S3 en formato: "fighters/{fighter_id}.{ext}"
        """
        return f"fighters/{fighter_id}.{file_ext}"

    async def upload_image(
        self,
        s3_key: str,
        image_data: bytes,
        content_type: str = "image/jpeg",
        metadata: Optional[dict] = None
    ) -> None:
        """
        Sube una imagen a S3

        Solo funciona en modo "s3". En modo "cache" lanza error.

        Args:
            s3_key: Key donde guardar la imagen en S3
            image_data: Bytes de la imagen
            content_type: MIME type de la imagen (default: image/jpeg)
            metadata: Metadata opcional para guardar con la imagen

        Raises:
            S3WriteNotAllowedError: Si estamos en modo cache (solo lectura)
            S3NotConfiguredError: Si S3 no está configurado
        """
        if self.is_read_only:
            raise S3WriteNotAllowedError(
                f"No se puede escribir en S3 en modo '{self.image_source_mode}'. "
                "Cambia IMAGE_SOURCE_MODE a 's3' para habilitar escritura."
            )

        # Preparar parámetros de upload
        upload_params = {
            "Bucket": self.aws_s3_bucket,
            "Key": s3_key,
            "Body": BytesIO(image_data),
            "ContentType": content_type,
            "CacheControl": "public, max-age=31536000",  # 1 año - las imágenes no cambian
        }

        # Agregar metadata si existe (convertir valores a strings)
        if metadata:
            # S3 user metadata only accepts US-ASCII values. Fighter names
            # remain Unicode in Mongo; only the auxiliary object metadata is
            # transliterated here.
            str_metadata = {
                k: unicodedata.normalize("NFKD", str(v))
                .encode("ascii", "ignore")
                .decode("ascii")
                for k, v in metadata.items()
            }
            upload_params["Metadata"] = str_metadata

        # Subir a S3
        self.s3_client.put_object(**upload_params)


# Instancia singleton del servicio
_s3_service_instance: Optional[S3Service] = None


def get_s3_service() -> S3Service:
    """
    Retorna la instancia singleton del servicio S3
    """
    global _s3_service_instance
    if _s3_service_instance is None:
        _s3_service_instance = S3Service()
    return _s3_service_instance
