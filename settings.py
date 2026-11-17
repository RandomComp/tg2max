"""Настройки моста: значения по умолчанию, переопределение из .env, реестр для UI.

Порядок приоритета: значение из sqlite (то, что натыкал в /settings) → переменная
окружения `BRIDGE_<КЛЮЧ>` из .env → значение по умолчанию отсюда.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

BOOL = "bool"
INT = "int"
STR = "str"

# Участки меню /settings
SEC_WHO = "От кого получать"
SEC_NOTIFY = "Уведомления"
SEC_OTHER = "Прочее"
SEC_MAIN = "Главный канал"


@dataclass(frozen=True)
class Option:
    key: str
    label: str
    section: str
    default: bool | int | str
    kind: str = BOOL
    hint: str = ""

    @property
    def env(self) -> str:
        return f"BRIDGE_{self.key.upper()}"


OPTIONS: tuple[Option, ...] = (
    # --- какой канал главный -------------------------------------------
    Option(
        "hub_channel",
        "Главный канал",
        SEC_MAIN,
        "telegram",
        kind=STR,
        hint="Сюда приходят сообщения из остальных каналов и отсюда уходят ответы",
    ),
    # --- от кого получать сообщения -----------------------------------
    Option(
        "get_channels",
        "Каналы",
        SEC_WHO,
        False,
        hint="Сообщения из каналов MAX идут в Telegram",
    ),
    Option(
        "get_service",
        "Системные аккаунты",
        SEC_WHO,
        False,
        hint="Безопасность, Коды подтверждения и прочие служебные чаты",
    ),
    Option(
        "get_calls",
        "Групповые звонки",
        SEC_WHO,
        True,
        hint="Чаты «Групповой звонок <дата>»",
    ),
    Option(
        "get_saved",
        "Избранное",
        SEC_WHO,
        True,
        hint="Чат с самим собой (id 0)",
    ),
    # --- уведомления ---------------------------------------------------
    Option("notify_read", "Статусы прочтения", SEC_NOTIFY, True, hint="MAX · Имя + [Прочитано]"),
    Option("notify_edit", "Правки сообщений", SEC_NOTIFY, True),
    Option("notify_delete", "Удаления сообщений", SEC_NOTIFY, True),
    Option("notify_reactions", "Реакции", SEC_NOTIFY, True),
    Option("notify_typing", "Печатает…", SEC_NOTIFY, True),
    # --- прочее --------------------------------------------------------
    Option(
        "forward_voice",
        "Голосовые и кружки",
        SEC_OTHER,
        True,
        hint="Пересылать файлом, а не пометкой",
    ),
    Option(
        "chats_view",
        "Вид списка чатов",
        SEC_OTHER,
        "short",
        kind=STR,
        hint="long — карточками с аватарками, short — списком имён",
    ),
    Option("auto_read", "Автоотметка прочитанным", SEC_OTHER, False, hint="Читаешь в главном — читается в MAX"),
    Option("quiet_hours", "Тихие часы", SEC_OTHER, False, hint="Не пересылать в TG в заданный интервал"),
    Option("quiet_from", "Тихие часы: с", SEC_OTHER, 23, kind=INT),
    Option("quiet_to", "Тихие часы: до", SEC_OTHER, 8, kind=INT),
    Option("limit_to_max", "Лимит в источник, МБ", SEC_OTHER, 20, kind=INT),
    Option("limit_to_tg", "Лимит из источника, МБ", SEC_OTHER, 48, kind=INT),
    Option(
        "max_inbox",
        "Куда падают чужие в MAX",
        SEC_MAIN,
        0,
        kind=INT,
        hint="0 — Избранное",
    ),
)

BY_KEY: dict[str, Option] = {option.key: option for option in OPTIONS}

# Кнопочные пресеты для числовых значений
LIMIT_PRESETS = (5, 10, 20, 50, 100, 200, 500, 2000)


def env_value(option: Option) -> bool | int | str | None:
    raw = os.getenv(option.env, "").strip()
    if not raw:
        return None
    if option.kind == BOOL:
        return raw.lower() in {"1", "true", "yes", "on", "да"}
    if option.kind == STR:
        return raw
    try:
        return int(raw)
    except ValueError:
        return None


def default_value(option: Option) -> bool | int | str:
    return option.default


def as_bool(value: bool | int | str | None, fallback: bool = False) -> bool:
    if value is None:
        return fallback
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "on", "да"}


def as_int(value: bool | int | str | None, fallback: int = 0) -> int:
    if value is None:
        return fallback
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except ValueError:
        return fallback


class Settings:
    """Эффективные значения настроек: sqlite → .env → умолчание.

    Значения из sqlite появляются, когда пользователь потыкал /settings;
    до этого берутся `BRIDGE_*` из .env, а если и их нет — умолчания выше.
    """

    def __init__(self, store) -> None:  # Store без импорта, чтобы не плодить циклы
        self._store = store
        self._cache: dict[str, bool | int | str] = {}
        self.from_env: set[str] = set()

    async def load(self) -> None:
        stored = await self._store.all_settings()
        self._cache = {}
        self.from_env = set()
        for option in OPTIONS:
            if option.key in stored:
                self._cache[option.key] = self._parse(option, stored[option.key])
                continue
            env = env_value(option)
            if env is not None:
                self._cache[option.key] = env
                self.from_env.add(option.key)
            else:
                self._cache[option.key] = default_value(option)

    @staticmethod
    def _parse(option: Option, raw: bool | int | str) -> bool | int | str:
        if option.kind == BOOL:
            return as_bool(raw)
        if option.kind == STR:
            return str(raw)
        return as_int(raw)

    async def set(self, key: str, value: bool | int | str) -> None:
        option = BY_KEY[key]
        self._cache[key] = value
        self.from_env.discard(key)
        if option.kind == BOOL:
            stored = str(int(bool(value)))
        elif option.kind == STR:
            stored = str(value)
        else:
            stored = str(int(value))
        await self._store.set_setting(key, stored)

    def bool(self, key: str) -> bool:
        return as_bool(self._cache.get(key), as_bool(default_value(BY_KEY[key])))

    def int(self, key: str) -> int:
        return as_int(self._cache.get(key), as_int(default_value(BY_KEY[key])))

    def text(self, key: str) -> str:
        value = self._cache.get(key, default_value(BY_KEY[key]))
        return str(value or "")

    def raw(self, key: str) -> bool | int | str:
        return self._cache.get(key, default_value(BY_KEY[key]))

    def quiet_now(self, hour: int) -> bool:
        """Попадает ли текущий час в тихие часы."""
        if not self.bool("quiet_hours"):
            return False
        start = self.int("quiet_from") % 24
        end = self.int("quiet_to") % 24
        if start == end:
            return False
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end
