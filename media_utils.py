"""Работа с картинками: аватарки жмём в маленький квадрат.

Общий модуль, чтобы сжатие было одинаковым у всех каналов: и MAX, и Telegram
тянут картинку по URL, но уменьшать её должен тот, кто отправляет.
"""

from __future__ import annotations

import logging
from io import BytesIO

from PIL import Image, ImageOps

log = logging.getLogger(__name__)

AVATAR_SIZE = 128
AVATAR_QUALITY = 85


def shrink_avatar(
    raw: bytes, size: int = AVATAR_SIZE, quality: int = AVATAR_QUALITY
) -> bytes:
    """Квадратная миниатюра size×size в JPEG. При ошибке возвращает исходное."""
    try:
        with Image.open(BytesIO(raw)) as source:
            image = ImageOps.exif_transpose(source)
            if image.mode not in ("RGB", "L"):
                image = image.convert("RGB")
            image = ImageOps.fit(image, (size, size), Image.Resampling.LANCZOS)
            out = BytesIO()
            image.save(out, format="JPEG", quality=quality, optimize=True)
            return out.getvalue()
    except Exception:
        log.debug("не смог сжать аватарку, отправляю как есть", exc_info=True)
        return raw


def avatar_cache_key(url: str, size: int = AVATAR_SIZE) -> str:
    """Ключ кэша file_id: с размером, чтобы после смены размера не отдавать старое."""
    return f"avatar:{size}:{url}"
