"""Telegram-интерфейс главного канала.

Здесь только то, что специфично для Telegram: клавиатура, команды и переход
действий меню в ядро. Меню (пункты и их действия) общие для всех каналов —
их строит ``bridge.py``, а этот модуль рисует их кнопками.
"""

from __future__ import annotations

import html
import logging
from collections.abc import Awaitable, Callable

from aiogram import BaseMiddleware, Bot, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from aiogram.types import Message as TgMessage
from aiogram.types import TelegramObject

from bridge import CHATS_VIEW_ALIASES, BridgeCore
from channels.telegram import TelegramChannel
from config import Config
from settings import BY_KEY, Settings
from store import Store

log = logging.getLogger(__name__)

BUTTON_CHATS = "Чаты"
BUTTON_PROFILE = "Профиль"
BUTTON_SETTINGS = "Настройки"
BUTTON_HELP = "Помощь"

HELP = (
    "<b>Мост мессенджеров</b>\n\n"
    "Сообщения из всех каналов приходят сюда: ответь на такое сообщение (reply) —\n"
    "уйдёт обратно в тот же чат того же мессенджера.\n\n"
    "/chats — чаты всех каналов: писать, история, игнорить\n"
    "/chats readonly — только те, куда писать нельзя\n"
    "/history — история переписки (активный чат или чат из reply)\n"
    "/settings — настройки кнопками; текстом можно так: /settings 16 200\n"
    "/to max:1234567890 текст — написать в конкретный чат конкретного канала\n"
    "/to +79991234567 [текст] — найти человека в MAX по номеру\n"
    "/stop — сбросить активный чат\n"
    "/me — что подключено и какой канал главный"
)


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BUTTON_CHATS), KeyboardButton(text=BUTTON_PROFILE)],
            [KeyboardButton(text=BUTTON_SETTINGS), KeyboardButton(text=BUTTON_HELP)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Ответь на сообщение или напиши в активный чат",
    )


class OwnerGate(BaseMiddleware):
    """Пускает только владельца. Если владелец не задан в .env — привязывает первого."""

    def __init__(self, store: Store, owner_id: int) -> None:
        self.store = store
        self.owner_id = owner_id

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict], Awaitable[object]],
        event: TelegramObject,
        data: dict,
    ) -> object | None:
        user = data.get("event_from_user")
        if user is None:
            return await handler(event, data)

        owner = self.owner_id or await self.store.get_owner()
        if owner is None:
            await self.store.set_owner(user.id)
            owner = user.id
            log.info("владелец бота привязан: %s", user.id)
            bot: Bot = data["bot"]
            await bot.send_message(
                user.id, f"Бот привязан к тебе (id {user.id}). Напиши /start."
            )

        if user.id != owner:
            log.warning("чужой пользователь %s попытался писать в бота", user.id)
            return None
        return await handler(event, data)


def build_router(
    bot: Bot,
    telegram: TelegramChannel,
    core: BridgeCore,
    store: Store,
    settings: Settings,
    cfg: Config,
) -> Router:
    router = Router(name="bridge")

    def channels_line() -> str:
        return ", ".join(
            f"{channel.display}{' (главный)' if name == core.hub_name else ''}"
            for name, channel in core.channels.items()
        )

    async def show_history_for(message: TgMessage) -> None:
        target = None
        if message.reply_to_message is not None:
            target = await core.reply_target(
                message.chat.id, message.reply_to_message.message_id
            )
        if target is None:
            active = await store.get_active(message.from_user.id)
            if active is not None:
                target = (active[0], active[1], active[2], None)
        if target is None:
            await message.answer("Чат не выбран: /chats — выбрать из списка.")
            return
        await core.show_menu(message.chat.id, f"history:{target[0]}:{target[1]}")

    # --- команды --------------------------------------------------------

    @router.message(Command("start", "help"))
    async def cmd_start(message: TgMessage) -> None:
        await message.answer(HELP, reply_markup=main_menu())

    @router.message(Command("me"))
    async def cmd_me(message: TgMessage) -> None:
        lines = [f"Главный канал: <b>{html.escape(core.display(core.hub_name))}</b>"]
        lines.append(f"Каналы: {html.escape(channels_line())}")
        active = await store.get_active(message.from_user.id)
        if active is not None:
            channel_name, _chat_id, title = active
            lines.append(
                f"Активный чат: {html.escape(core.display(channel_name))} · "
                f"{html.escape(title)}"
            )
        else:
            lines.append("Активный чат не выбран")
        muted = await store.muted_chats()
        lines.append(f"Игнорируемых чатов: {len(muted)}")
        await message.answer("\n".join(lines))

    @router.message(Command("chats"))
    async def cmd_chats(message: TgMessage, command: CommandObject | None = None) -> None:
        args = ((command.args if command else None) or "").strip()
        mode = args.split(" ")[0].lower() if args else ""
        if mode in CHATS_VIEW_ALIASES:
            await settings.set("chats_view", CHATS_VIEW_ALIASES[mode])
            await message.answer(
                f"Вид списка чатов: {core.option_value('chats_view')}",
                parse_mode=None,
            )
        menu_id = "chats:readonly" if mode.startswith("read") or mode == "ro" else "chats"
        await core.show_menu(message.chat.id, menu_id)

    @router.message(Command("settings"))
    async def cmd_settings(message: TgMessage, command: CommandObject | None = None) -> None:
        args = ((command.args if command else None) or "").strip()
        if args:
            await message.answer(await core.apply_setting(args), parse_mode=None)
        await core.show_menu(message.chat.id, "settings")

    @router.message(Command("history", "hist"))
    async def cmd_history(message: TgMessage) -> None:
        await show_history_for(message)

    @router.message(Command("stop"))
    async def cmd_stop(message: TgMessage) -> None:
        await store.clear_active(message.from_user.id)
        await message.answer("Активный чат сброшен.", reply_markup=main_menu())

    @router.message(Command("cancel"))
    async def cmd_cancel(message: TgMessage) -> None:
        await message.answer(await core.cancel_input(message.chat.id), parse_mode=None)

    @router.message(Command("to"))
    async def cmd_to(message: TgMessage, command: CommandObject) -> None:
        args = (command.args or "").strip()
        if not args:
            await message.answer(
                "Формат: /to канал:чат [текст] или /to +79991234567 [текст]"
            )
            return
        target, _, text = args.partition(" ")

        if ":" in target:
            if not target.split(":", 1)[1].lstrip("-").isdigit():
                await message.answer("Формат: /to канал:чат, например /to max:367115027")
                return
            ref = await core.find_chat(target)
            channel_name, _, raw_chat = target.partition(":")
            chat_id = int(raw_chat)
            title = ref.title if ref else target
        else:
            max_channel = core.channels.get("max")
            if max_channel is None or not hasattr(max_channel, "resolve"):
                await message.answer("По номеру искать умеет только MAX")
                return
            try:
                chat_id, title = await max_channel.resolve(target)
            except Exception as exc:  # noqa: BLE001
                await message.answer(f"Не нашёл «{target}»: {exc}", parse_mode=None)
                return
            channel_name = "max"

        if not text.strip():
            await store.set_active(message.from_user.id, channel_name, chat_id, title)
            await message.answer(
                f"Активный чат: <b>{html.escape(title)}</b>\nПиши текст — уйдёт туда.",
                reply_markup=main_menu(),
            )
            return

        await core.send_out(channel_name, chat_id, title, text=text)
        await message.answer(f"→ {title}", parse_mode=None)

    # --- кнопки меню: одно действие на любой callback -------------------

    @router.callback_query()
    async def on_action(call: CallbackQuery) -> None:
        action = call.data or ""
        chat_id = call.message.chat.id if call.message is not None else 0
        try:
            result = await core.apply_action(action, chat_id, call.from_user.id)
        except Exception as exc:  # noqa: BLE001 - показываем причину в TG
            log.exception("ошибка действия %s", action)
            await call.answer(f"Не получилось: {exc}"[:200])
            return
        await call.answer((result or "")[:200])

        # выбрал чат — сразу даём к нему историю и игнор
        if action.startswith("pick:") and result:
            group = action.split(":", 1)[1]
            try:
                keyboard = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="История", callback_data=f"hist:{group}:0"
                            ),
                            InlineKeyboardButton(
                                text="Игнорить", callback_data=f"mute:{group}"
                            ),
                        ]
                    ]
                )
                await bot.send_message(chat_id, result, parse_mode=None, reply_markup=keyboard)
            except Exception:
                log.debug("не смог добавить кнопки к подтверждению", exc_info=True)

        if call.message is not None and action not in {"noop"}:
            await core.refresh_menu(chat_id, call.message.message_id)

    # --- меню на клавиатуре ---------------------------------------------

    @router.message(F.text.in_({BUTTON_CHATS, BUTTON_PROFILE, BUTTON_HELP, BUTTON_SETTINGS}))
    async def on_menu_button(message: TgMessage) -> None:
        if message.text == BUTTON_CHATS:
            await cmd_chats(message)
        elif message.text == BUTTON_PROFILE:
            await cmd_me(message)
        elif message.text == BUTTON_SETTINGS:
            await cmd_settings(message)
        else:
            await cmd_start(message)

    # --- сообщения владельца --------------------------------------------

    @router.message(F.contact)
    async def on_contact(message: TgMessage) -> None:
        contact = message.contact
        phone = (contact.phone_number or "").strip() if contact else ""
        if not phone:
            await message.reply("В этом контакте нет номера телефона.")
            return
        if not phone.startswith("+"):
            phone = f"+{phone}"

        max_channel = core.channels.get("max")
        if max_channel is None or not hasattr(max_channel, "resolve"):
            await message.answer("Поиск по номеру умеет только MAX")
            return
        try:
            chat_id, title = await max_channel.resolve(phone)
        except Exception as exc:  # noqa: BLE001
            await message.answer(f"Не нашёл в MAX номер {phone}: {exc}", parse_mode=None)
            return

        await store.set_active(message.from_user.id, "max", chat_id, title)
        name = " ".join(part for part in (contact.first_name, contact.last_name) if part)
        who = f"«{html.escape(name)}» " if name else ""
        await message.answer(
            f"Контакт {who}в MAX: <b>{html.escape(title)}</b>\n"
            f"Активный чат переключён — пиши текст, фото или видео.",
            reply_markup=main_menu(),
        )

    async def relay(message: TgMessage, kind: str = "text") -> None:
        """Сообщение владельца -> в активный чат или в чат из reply."""
        reply = message.reply_to_message
        target = await core.hub_target(
            message.chat.id,
            reply.message_id if reply is not None else None,
            message.from_user.id if message.from_user else 0,
        )
        if target is None:
            await message.answer(
                "Чат не выбран: /chats — выбрать, или ответь на сообщение из нужного чата."
            )
            return

        channel_name, chat_id, title, reply_source = target
        limit = settings.int("limit_to_max")
        media = await telegram.download_media(message, limit * 1024 * 1024)
        if kind != "text" and not media:
            await message.reply(f"Не смог переслать файл (лимит {limit} МБ).")
            return
        await core.send_out(
            channel_name,
            chat_id,
            title,
            text=(message.text or message.caption or "").strip(),
            media=media,
            reply_to=reply_source,
        )

    @router.message(F.text)
    async def on_text(message: TgMessage) -> None:
        if not message.text or message.text.startswith("/"):
            await message.answer("Не знаю такую команду. /help")
            return
        # если настройка ждёт новое значение — это сообщение и есть значение
        applied = await core.apply_input(message.chat.id, message.text)
        if applied is not None:
            await message.answer(applied, parse_mode=None)
            return
        await relay(message)

    @router.message(F.photo | F.document | F.video | F.animation | F.voice | F.video_note)
    async def on_media(message: TgMessage) -> None:
        if not settings.bool("forward_voice") and (message.voice or message.video_note):
            await message.reply("Пересылка голосовых и кружков выключена — включи в /settings.")
            return
        await relay(message, kind="media")

    @router.message(F.sticker | F.audio)
    async def on_other(message: TgMessage) -> None:
        await message.reply(
            "Стикеры и аудио пока не пересылаю. Можно: текст, фото, видео, файлы, голосовые и кружки."
        )

    router.message.middleware(OwnerGate(store, cfg.owner_id))
    router.callback_query.middleware(OwnerGate(store, cfg.owner_id))
    return router
