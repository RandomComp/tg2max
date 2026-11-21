"""Точка входа: каналы мессенджеров + ядро моста.

Один канал назначается главным (настройка ``hub_channel``): в нём владелец читает
сообщения из всех остальных каналов и отвечает на них. Остальные каналы —
источники: отдают свои события ядру и получают от него ответы и действия.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode

from bot import build_router
from bridge import BridgeCore
from channels.max import MaxChannel
from channels.telegram import TelegramChannel
from config import load_config
from max_side import MaxBridge
from settings import Settings
from store import Store

log = logging.getLogger("bridge")


def setup_logging(level: str, log_file) -> None:  # путь к файлу лога
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)


async def main() -> None:
    cfg = load_config()
    setup_logging(cfg.log_level, cfg.work_dir / "bridge.log")

    store = Store(cfg.work_dir / "state.sqlite3")
    await store.init()

    settings = Settings(store)
    await settings.load()

    # Локальный Bot API (telegram-bot-api --local): TG-трафик тянет демон,
    # самому боту VPN не нужен, а MAX может ходить напрямую.
    session = None
    if cfg.tg_api_base:
        session = AiohttpSession(api=TelegramAPIServer.from_base(cfg.tg_api_base))
        log.info("Telegram Bot API: %s", cfg.tg_api_base)

    bot = Bot(
        cfg.bot_token,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    me = await bot.get_me()

    owner = cfg.owner_id or await store.get_owner() or 0
    max_bridge = MaxBridge(cfg.max_phone, cfg.work_dir, cfg.session_name)

    telegram = TelegramChannel(bot)
    telegram.set_avatar_cache(store.get_avatar_file, store.set_avatar_file)
    max_channel = MaxChannel(max_bridge)

    channels = {telegram.name: telegram, max_channel.name: max_channel}

    core = BridgeCore(
        channels,
        store,
        settings,
        inbox={
            "telegram": owner,
            "max": settings.int("max_inbox"),
        },
    )

    async def on_max_ready(profile: str) -> None:
        await core.hub.send_text(
            await core.hub_inbox(), f"{', '.join(core.clients)} подключён(-ы): {profile}"
        )

    max_bridge.on_ready = on_max_ready

    dp = Dispatcher()
    dp.include_router(build_router(bot, telegram, core, store, settings, cfg))

    await max_channel.start()
    log.info(
        "бот @%s запущен, каналы: %s, главный: %s",
        me.username,
        ", ".join(channels),
        core.hub_name,
    )

    try:
        await dp.start_polling(bot)
    finally:
        await max_channel.stop()
        await telegram.stop()
        await store.close()
        log.info("остановлено")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
