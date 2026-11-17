"""Настройки моста Telegram -> MAX (читаются из .env рядом с проектом)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


@dataclass(frozen=True)
class Config:
    bot_token: str
    owner_id: int
    max_phone: str
    work_dir: Path
    session_name: str
    log_level: str
    tg_api_base: str


def load_config() -> Config:
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Не задан TG_BOT_TOKEN в .env — возьми токен у @BotFather")

    phone = os.getenv("MAX_PHONE", "").strip()
    if not phone:
        raise SystemExit("Не задан MAX_PHONE в .env — номер аккаунта MAX в формате +7...")

    owner_raw = os.getenv("OWNER_TG_ID", "").strip()
    try:
        owner_id = int(owner_raw) if owner_raw else 0
    except ValueError:
        raise SystemExit("OWNER_TG_ID должен быть числом") from None

    work_dir = BASE_DIR / "data"
    work_dir.mkdir(exist_ok=True)

    return Config(
        bot_token=token,
        owner_id=owner_id,
        max_phone=phone,
        work_dir=work_dir,
        session_name=os.getenv("MAX_SESSION", "max.db").strip() or "max.db",
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        tg_api_base=os.getenv("TG_API_BASE", "").strip(),
    )
