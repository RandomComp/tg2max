"""Канал Telegram: обёртка над aiogram, приводящая его к общему интерфейсу."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from pathlib import Path
from typing import Sequence

import aiohttp
from aiogram import Bot
from aiogram.types import (
    BufferedInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    ReactionTypeEmoji,
)
from aiogram.types import Message as TgMessage

from media_utils import avatar_cache_key, shrink_avatar

from .base import (
    DOCUMENT,
    PHOTO,
    VIDEO,
    VOICE,
    ChatRef,
    Incoming,
    Media,
    MenuItem,
)

log = logging.getLogger(__name__)

CAPTION_LIMIT = 1000
TG_TEXT_LIMIT = 3900


def _markup(items: Sequence[MenuItem]) -> InlineKeyboardMarkup | None:
    """Кнопки из пунктов меню: по две в ряд, но пункт с full_row — отдельной строкой."""
    rows: list[list[InlineKeyboardButton]] = []
    current: list[InlineKeyboardButton] = []

    def button(item: MenuItem) -> InlineKeyboardButton:
        if item.url:
            return InlineKeyboardButton(text=item.label[:60], url=item.url)
        return InlineKeyboardButton(text=item.label[:60], callback_data=item.action[:64])

    for item in items:
        if item.full_row:
            if current:
                rows.append(current)
                current = []
            rows.append([button(item)])
            continue
        current.append(button(item))
        if len(current) == 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


class TelegramChannel:
    """Telegram как канал: отправка сюда, приём отсюда."""

    name = "telegram"
    display = "TG"
    supports_buttons = True

    def __init__(self, bot: Bot, *, on_message=None, on_change=None) -> None:
        self.bot = bot
        self.on_message = on_message
        self.on_change = on_change
        self._avatar_get = None
        self._avatar_set = None
        self._avatars: dict[str, bytes | None] = {}

    def set_avatar_cache(self, getter, setter) -> None:  # noqa: ANN001 - функции хранилища
        """Кэш file_id аватарок: повторно не заливаем одну и ту же картинку."""
        self._avatar_get = getter
        self._avatar_set = setter

    # --- жизненный цикл -------------------------------------------------

    async def start(self) -> None:
        """Приём апдейтов запускает диспетчер в main.py."""

    async def stop(self) -> None:
        await self.bot.session.close()

    # --- отправка -------------------------------------------------------

    async def send_text(
        self, chat_id: int, text: str, reply_to: int | None = None
    ) -> int | None:
        sent = await self.bot.send_message(
            chat_id, text[:TG_TEXT_LIMIT], reply_to_message_id=reply_to, parse_mode=None
        )
        return sent.message_id

    async def send_media(
        self,
        chat_id: int,
        media: Sequence[Media],
        caption: str | None = None,
        reply_to: int | None = None,
    ) -> list[int]:
        photos = [item for item in media if item.kind == PHOTO]
        videos = [item for item in media if item.kind == VIDEO]
        voices = [item for item in media if item.kind == VOICE]
        documents = [
            item for item in media if item.kind in (DOCUMENT, "file", "", "sticker", "attachment")
        ]
        album = photos + videos
        head = (caption or "")[:CAPTION_LIMIT]
        sent_ids: list[int] = []

        def album_item(item: Media, with_caption: bool):
            file = BufferedInputFile(item.data, item.name)
            text = head if with_caption else None
            if item.kind == VIDEO:
                return InputMediaVideo(media=file, caption=text, parse_mode=None)
            return InputMediaPhoto(media=file, caption=text, parse_mode=None)

        if len(album) == 1:
            item = album[0]
            file = BufferedInputFile(item.data, item.name)
            if item.kind == PHOTO:
                sent = await self.bot.send_photo(
                    chat_id, file, caption=head or None, reply_to_message_id=reply_to,
                    parse_mode=None,
                )
            else:
                sent = await self.bot.send_video(
                    chat_id, file, caption=head or None, reply_to_message_id=reply_to,
                    parse_mode=None, supports_streaming=True,
                )
            sent_ids.append(sent.message_id)
        elif album:
            group = [album_item(item, index == 0) for index, item in enumerate(album[:10])]
            for sent in await self.bot.send_media_group(
                chat_id, group, reply_to_message_id=reply_to
            ):
                sent_ids.append(sent.message_id)

        for item in voices:
            file = BufferedInputFile(item.data, item.name)
            caption_text = head if not sent_ids else None
            if item.name.endswith(".ogg"):
                sent = await self.bot.send_voice(
                    chat_id, file, caption=caption_text, reply_to_message_id=reply_to,
                    parse_mode=None,
                )
            else:
                sent = await self.bot.send_document(
                    chat_id, file, caption=caption_text, reply_to_message_id=reply_to,
                    parse_mode=None,
                )
            sent_ids.append(sent.message_id)

        for item in documents:
            file = BufferedInputFile(item.data, item.name)
            caption_text = head if not sent_ids else None
            sent = await self.bot.send_document(
                chat_id, file, caption=caption_text, reply_to_message_id=reply_to,
                parse_mode=None,
            )
            sent_ids.append(sent.message_id)

        return sent_ids

    async def show_typing(self, chat_id: int) -> None:
        try:
            await self.bot.send_chat_action(chat_id, "typing")
        except Exception:
            log.debug("не смог показать «печатает»", exc_info=True)

    async def download_avatar(self, url: str) -> bytes | None:
        key = avatar_cache_key(url) if url else ""
        file_id = None
        if key and self._avatar_get is not None:
            file_id = await self._avatar_get(key)
        if file_id:
            try:
                await self.bot.send_photo(
                    chat_id,
                    file_id,
                    caption=caption,
                    parse_mode=None,
                    reply_markup=_markup(group_items),
                )
                return None
            except Exception:
                log.debug("file_id аватарки %s устарел", url, exc_info=True)

        return await self.avatar_bytes(url)

    async def send_menu(
        self,
        chat_id: int,
        text: str,
        items: Sequence[MenuItem],
        edit_message_id: int | None = None,
    ) -> int | None:
        """Меню кнопками; пункты с группой рисуются карточками с аватаркой."""
        if edit_message_id is not None:
            edited = await self._edit_menu(chat_id, edit_message_id, text, items)
            if edited:
                return edit_message_id

        groups: dict[str, list[MenuItem]] = {}
        for item in items:
            groups.setdefault(item.group or "", []).append(item)

        rich = any(key for key in groups)
        if not rich:
            sent = await self.bot.send_message(
                chat_id, text[:TG_TEXT_LIMIT], parse_mode=None, reply_markup=_markup(items)
            )
            return sent.message_id

        log.debug(f"{groups=}")

        header = await self.bot.send_message(chat_id, text[:TG_TEXT_LIMIT], parse_mode=None)
        for key, group_items in groups.items():
            if not key:
                with suppress(Exception):
                    await self.bot.send_message(
                        chat_id,
                        "Или выбери из списка:",
                        parse_mode=None,
                        reply_markup=_markup(group_items),
                    )
                continue
            caption = (group_items[0].title or group_items[0].label)[:1020]


            url = group_items[0].avatar_url

            key = avatar_cache_key(url) if url else ""
            file_id = None
            if key and self._avatar_get is not None:
                file_id = await self._avatar_get(key)
            if file_id:
                try:
                    await self.bot.send_photo(
                        chat_id,
                        file_id,
                        caption=caption,
                        parse_mode=None,
                        reply_markup=_markup(group_items),
                    )
                    continue
                except Exception:
                    log.debug("file_id аватарки %s устарел", url, exc_info=True)

            data = await self.avatar_bytes(url)

            if data:
                sent = await self.bot.send_photo(
                    chat_id,
                    BufferedInputFile(data, filename="avatar.jpg"),
                    caption=caption,
                    parse_mode=None,
                    reply_markup=_markup(group_items),
                )
                if key and sent.photo and self._avatar_set is not None:
                    await self._avatar_set(key, sent.photo[-1].file_id)
            else:
                await self.bot.send_message(
                    chat_id, caption, parse_mode=None, reply_markup=_markup(group_items)
                )
            await asyncio.sleep(0.3)
        return header.message_id

    async def react(self, chat_id: int, message_id: int, emoji: str | None) -> None:
        try:
            if emoji:
                await self.bot.set_message_reaction(
                    chat_id, message_id, reaction=[ReactionTypeEmoji(emoji=emoji)]
                )
            else:
                await self.bot.set_message_reaction(chat_id, message_id, reaction=[])
        except Exception:
            log.debug("не смог поставить реакцию в Telegram", exc_info=True)

    async def mark_read(self, chat_id: int, message_id: int) -> None:
        """Telegram Bot API не даёт отмечать прочитанным — это не нужно и не умеет."""

    async def edit_text(self, chat_id: int, message_id: int, text: str) -> None:
        try:
            await self.bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, text=text[:TG_TEXT_LIMIT],
                parse_mode=None,
            )
        except Exception:
            log.debug("не смог изменить сообщение в Telegram", exc_info=True)

    async def delete(self, chat_id: int, message_ids: Sequence[int]) -> None:
        for message_id in message_ids:
            try:
                await self.bot.delete_message(chat_id, message_id)
            except Exception:
                log.debug("не смог удалить сообщение %s", message_id, exc_info=True)

    async def list_chats(self) -> list[ChatRef]:
        """У Telegram-канала чат один — этот, с ботом."""
        return []

    async def chat_flags(self, chat_id: int) -> dict[str, bool]:
        """У Telegram-канала особых типов чатов нет."""
        return {}

    async def copy_message(self, chat_id: int, message_id: int) -> bool:
        """Показать сообщение копией (для истории)."""
        try:
            await self.bot.copy_message(chat_id, chat_id, message_id)
            return True
        except Exception:
            log.debug("не смог скопировать сообщение %s", message_id, exc_info=True)
            return False

    async def chat_title(self, channel_name: str, chat_id: int) -> str:
        return str(chat_id)

    async def avatar_bytes(self, url: str | None) -> bytes | None:
        """Аватарка для карточки: качаем и жмём в 128×128 (кэш по URL)."""
        if not url:
            return None
        if url in self._avatars:
            return self._avatars[url]
        raw = await self._fetch_avatar(url)
        if raw is None:
            self._avatars[url] = None
            return None
        data = await asyncio.to_thread(shrink_avatar, raw)
        log.debug("аватарка %s: %d КБ -> %d КБ", url, len(raw) // 1024, len(data) // 1024)
        self._avatars[url] = data
        return data

    async def _edit_menu(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        items: Sequence[MenuItem],
    ) -> bool:
        """Правит сообщение меню одним запросом — вместе с кнопками.

        Отдельный edit_message_text стирает клавиатуру, поэтому передаём разметку
        сразу. False — если править нельзя (например, это фото-карточка).
        """
        try:
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text[:TG_TEXT_LIMIT],
                parse_mode=None,
                reply_markup=_markup(items),
            )
            return True
        except Exception as exc:  # noqa: BLE001 - решаем по тексту ошибки
            if "not modified" in str(exc).lower():
                return True
            log.debug("не смог изменить меню, покажу новым сообщением", exc_info=True)
            return False

    async def _fetch_avatar(self, url: str) -> bytes | None:
        try:
            async with aiohttp.ClientSession(
                headers={"User-Agent": "Mozilla/5.0"}
            ) as session:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        return None
                    return await response.read()
        except Exception:
            log.debug("не смог скачать аватарку %s", url, exc_info=True)
            return None

    async def download_media(self, message: TgMessage, limit_bytes: int) -> list[Media]:
        """Скачивает вложения сообщения владельца в нейтральный вид."""
        if message.photo:
            target, name, kind = message.photo[-1], "photo.jpg", PHOTO
        elif message.video or message.animation:
            target = message.video or message.animation
            name, kind = "video.mp4", VIDEO
        elif message.voice or message.video_note:
            target = message.voice or message.video_note
            name, kind = "voice.ogg", VOICE
        elif message.audio:
            target = message.audio
            name, kind = target.file_name, VOICE
        elif message.document:
            target = message.document
            name, kind = message.document.file_name or "file", DOCUMENT
        else:
            return []

        size = getattr(target, "file_size", 0) or 0
        if limit_bytes and size > limit_bytes:
            log.warning("файл из Telegram больше лимита: %s байт", size)
            return []

        log.debug(f"{target=}")

        try:
            file = await message.bot.get_file(target.file_id)

            buffer = Path(file.file_path).read_bytes()
        except Exception:
            log.debug("не смог скачать файл из Telegram", exc_info=True)
            return []
        return [Media(kind, name, buffer)]
