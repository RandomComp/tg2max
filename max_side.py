"""Обёртка над PyMax: вход в аккаунт MAX, отправка сообщений и входящие события."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp
from pymax import Client, Message
from pymax.types.domain.chat import Chat
from pymax.types.domain.user import User

from media_utils import AVATAR_QUALITY, AVATAR_SIZE, shrink_avatar

log = logging.getLogger(__name__)

MAX_MEDIA = 10
MAX_UPLOAD = 48 * 1024 * 1024

# MAX сам создаёт чат «Групповой звонок <дата>» под каждый звоно́к: свежий показываем,
# старые прячем из /chats, чтобы список не забивался мусором.
GROUP_CALL_PREFIX = "групповой звонок"
GROUP_CALL_KEEP_MS = 24 * 60 * 60 * 1000

# Бит в Chat.restrictions: выставлен у каналов и чатов, где пишут только админы.
WRITE_RESTRICTION_BIT = 32

# Служебные чаты/аккаунты MAX («Безопасность» и подобное) в списках не показываем.
SERVICE_CHAT_OPTION = "SERVICE_CHAT"
SERVICE_USER_OPTIONS = {"SERVICE_ACCOUNT", "BOT"}


def chat_kind_of(chat: Chat) -> str:
    kind = getattr(chat, "type", "")
    return str(getattr(kind, "value", kind))


def chat_is_service(chat: Chat) -> bool:
    options = getattr(chat, "options", None)
    return isinstance(options, dict) and bool(options.get(SERVICE_CHAT_OPTION))


def chat_is_channel(chat: Chat) -> bool:
    return chat_kind_of(chat) == "CHANNEL"


def chat_is_readonly(chat: Chat) -> bool:
    """В чат нельзя писать: канал или ограничение «пишут только админы»."""
    if chat_is_channel(chat):
        return True
    restrictions = getattr(chat, "restrictions", None) or 0
    return bool(restrictions & WRITE_RESTRICTION_BIT)


def chat_is_stale_call(chat: Chat) -> bool:
    title = (chat.title or "").strip().lower()
    if not title.startswith(GROUP_CALL_PREFIX):
        return False
    age_ms = time.time() * 1000 - (chat.last_event_time or 0)
    return age_ms > GROUP_CALL_KEEP_MS


def chat_is_call(chat: Chat) -> bool:
    """Чат «Групповой звонок <дата>» — любого возраста."""
    return (chat.title or "").strip().lower().startswith(GROUP_CALL_PREFIX)

IncomingHandler = Callable[[Message, Client], Awaitable[None]]
ReadHandler = Callable[[Any], Awaitable[None]]


@dataclass
class IncomingMedia:
    """Вложение из MAX, готовое к отправке в Telegram."""

    kind: str  # "photo" | "document"
    name: str
    data: bytes


class MaxNotReady(RuntimeError):
    """MAX-клиент ещё не подключился (или упал)."""


def user_name(user: User | None, fallback_id: int | None = None) -> str:
    if user is not None:
        names = user.names or []
        for item in names:
            if item.name:
                return item.name
        for item in names:
            parts = [item.first_name, item.last_name]
            full = " ".join(part for part in parts if part)
            if full:
                return full
    return f"id{fallback_id}" if fallback_id is not None else "?"


def forwarded_message(message: Message) -> Message | None:
    """Оригинал пересланного сообщения (у форвардов вложения лежат в link.message)."""
    link = getattr(message, "link", None)
    if link is None:
        return None
    kind = getattr(link, "type", None)
    kind = getattr(kind, "value", kind)
    if str(kind) != "FORWARD":
        return None
    return getattr(link, "message", None)


def all_attaches(message: Message, depth: int = 3) -> list:
    """Вложения сообщения вместе с вложениями пересланных оригиналов."""
    result = list(message.attaches or [])
    origin = forwarded_message(message)
    if origin is not None and depth > 1:
        result.extend(all_attaches(origin, depth - 1))
    return result


def attachment_note(message: Message) -> str:
    """Человеческая пометка о вложениях, которые не пересылаются файлами."""
    labels = {
        "AUDIO": "аудио",
        "CONTACT": "контакт",
        "CALL": "звонок",
        "SHARE": "пересланное сообщение",
        "POLL": "опрос",
        "UNKNOWN": "вложение",
    }
    counts: dict[str, int] = {}
    for attach in all_attaches(message):
        kind = getattr(attach, "type", None)
        kind = getattr(kind, "value", kind)
        if str(kind) not in labels:
            continue  # фото/файлы/видео пересылаются файлами
        label = labels[str(kind)]
        counts[label] = counts.get(label, 0) + 1

    if not counts:
        return ""
    parts = [f"{name} ×{n}" if n > 1 else name for name, n in counts.items()]
    return "[" + ", ".join(parts) + "]"


class MaxBridge:
    def __init__(
        self,
        phone: str,
        work_dir: Path,
        session_name: str,
        on_incoming: IncomingHandler | None = None,
        on_ready: Callable[[str], Awaitable[None]] | None = None,
        on_read: ReadHandler | None = None,
        on_edit: IncomingHandler | None = None,
        on_delete: ReadHandler | None = None,
        on_typing: ReadHandler | None = None,
        on_reaction: ReadHandler | None = None,
    ) -> None:
        self.client = Client(
            phone=phone,
            work_dir=str(work_dir),
            session_name=session_name,
        )
        self.on_incoming = on_incoming
        self.on_ready = on_ready
        self.on_read = on_read
        self.on_edit = on_edit
        self.on_delete = on_delete
        self.on_typing = on_typing
        self.on_reaction = on_reaction
        self.me_id: int | None = None
        self.ready = asyncio.Event()
        self.error: BaseException | None = None
        self._chats: dict[int, Chat] = {}
        self._users: dict[int, User] = {}
        self._avatars: dict[str, bytes | None] = {}
        self._http: aiohttp.ClientSession | None = None
        self.last_list: dict[int, str] = {}
        self._missing_users: set[int] = set()
        self._register_handlers()

    # --- жизненный цикл -------------------------------------------------

    def _register_handlers(self) -> None:
        client = self.client

        @client.on_start()
        async def _on_start(c: Client) -> None:
            me = c.me
            self.me_id = me.contact.id if me and me.contact else None
            log.info("MAX подключён, id=%s", self.me_id)
            self._remember_chats(c.chats or [])
            for user in c.contacts or []:
                if user is not None:
                    self._users[user.id] = user
            self.ready.set()
            if self.on_ready is not None:
                try:
                    await self.on_ready(self.profile_label())
                except Exception:
                    log.exception("не смог сообщить в Telegram о подключении MAX")

        @client.on_message_read()
        async def _on_message_read(event: Any, c: Client) -> None:
            if getattr(event, "set_as_unread", False):
                return
            if self.on_read is None:
                return
            try:
                await self.on_read(event)
            except Exception:
                log.exception("не смог обработать отметку прочтения")

        @client.on_typing()
        async def _on_typing(event: Any, c: Client) -> None:
            if self.on_typing is None:
                return
            try:
                await self.on_typing(event)
            except Exception:
                log.exception("не смог обработать «печатает»")

        @client.on_reaction_update()
        async def _on_reaction(event: Any, c: Client) -> None:
            if self.on_reaction is None:
                return
            try:
                await self.on_reaction(event)
            except Exception:
                log.exception("не смог обработать реакцию")

        @client.on_message_edit()
        async def _on_message_edit(message: Message, c: Client) -> None:
            if message.chat_id is None:
                return
            if self.on_edit is None:
                return
            try:
                await self.on_edit(message, c)
            except Exception:
                log.exception("не смог переслать правку сообщения")

        @client.on_message_delete()
        async def _on_message_delete(event: Any, c: Client) -> None:
            if getattr(event, "chat_id", None) is None:
                return
            if self.on_delete is None:
                return
            try:
                await self.on_delete(event, c)
            except Exception:
                log.exception("не смог переслать удаление сообщения")

        @client.on_message()
        async def _on_message(message: Message, c: Client) -> None:
            if message.chat_id is None:
                return
            if self.on_incoming is None:
                return
            try:
                await self.on_incoming(message, c)
            except Exception:
                log.exception("не смог переслать входящее сообщение в Telegram")

    async def run(self) -> None:
        """Держит соединение с MAX (библиотека сама переподключается)."""
        try:
            await self.client.start()
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  #  нужно отпустить ожидающих
            self.error = exc
            self.ready.set()
            log.exception("MAX клиент остановился")
            raise

    async def ensure_ready(self, timeout: float = 20.0) -> None:
        if not self.ready.is_set():
            try:
                await asyncio.wait_for(self.ready.wait(), timeout)
            except TimeoutError:
                raise MaxNotReady("MAX ещё не подключён, подожди пару секунд") from None
        if self.error is not None:
            raise MaxNotReady(f"MAX клиент упал: {self.error}")

    # --- имена и чаты ---------------------------------------------------

    def _remember_chats(self, chats: Sequence[Chat]) -> None:
        for chat in chats:
            self._chats[chat.id] = chat

    async def get_user(self, user_id: int | None) -> User | None:
        if user_id is None:
            return None
        if user_id in self._users:
            return self._users[user_id]
        cached = self.client.get_cached_user(user_id)
        if cached is not None:
            self._users[user_id] = cached
            return cached
        try:
            user = await self.client.get_user(user_id)
        except Exception:
            log.debug("не смог загрузить пользователя %s", user_id, exc_info=True)
            return None
        if user is not None:
            self._users[user_id] = user
        return user

    async def user_label(self, user_id: int | None) -> str:
        return user_name(await self.get_user(user_id), user_id)

    async def chat_entry(self, chat: Chat) -> tuple[int, str, str | None]:
        """(chat_id, подпись, url аватарки)."""
        if self.is_saved_chat(chat):
            return chat.id, "Избранное", None
        if chat.title:
            return chat.id, chat.title, chat.base_icon_url or chat.base_raw_icon_url
        others = [uid for uid in (chat.participants or {}) if uid != self.me_id]
        user = await self.get_user(others[0]) if others else None
        fallback = others[0] if others else chat.id
        label = user_name(user, fallback)
        return chat.id, label, (user.base_url or user.base_raw_url) if user else None

    def is_saved_chat(self, chat: Chat) -> bool:
        """«Избранное» — диалог с самим собой (в MAX это чат с id 0)."""
        if chat_kind_of(chat) != "DIALOG":
            return False
        participants = chat.participants or {}
        if not participants:
            return chat.id == 0
        if chat.id == 0:
            return True
        return not [uid for uid in participants if uid != self.me_id]

    def is_service_dialog(self, chat: Chat) -> bool:
        """Диалог со служебным аккаунтом MAX (проверка по кэшу, без сети)."""
        if chat_kind_of(chat) != "DIALOG":
            return False
        peers = [uid for uid in (chat.participants or {}) if uid != self.me_id]
        if len(peers) != 1:
            return False
        user = self._users.get(peers[0])
        if user is None:
            return False
        return any(option in SERVICE_USER_OPTIONS for option in (user.options or []))

    async def label_for(self, message: Message) -> tuple[str, str | None]:
        """(подпись, url аватарки) для входящего сообщения."""
        chat = self._chats.get(message.chat_id or 0)
        if chat is not None and chat.title:
            return chat.title, chat.base_icon_url or chat.base_raw_icon_url
        user = await self.get_user(message.sender)
        avatar = (user.base_url or user.base_raw_url) if user else None
        return user_name(user, message.sender), avatar

    async def chat_kind(self, chat_id: int) -> str:
        """Тип чата MAX: DIALOG / CHAT / CHANNEL (из кэша или запросом)."""
        chat = await self.get_chat(chat_id)
        if chat is None:
            return ""
        return chat_kind_of(chat)

    async def get_chat(self, chat_id: int) -> Chat | None:
        """Чат по id: сначала из кэша, потом запросом."""
        chat = self._chats.get(chat_id)
        if chat is not None:
            return chat
        try:
            chat = await self.client.get_chat(chat_id)
        except Exception:
            log.debug("не смог получить чат %s", chat_id, exc_info=True)
            return None
        if chat is not None:
            self._chats[chat.id] = chat
        return chat

    async def chat_flags(self, chat_id: int) -> dict[str, bool]:
        """Признаки чата для настроек: канал, служебный, звонок, Избранное."""
        chat = await self.get_chat(chat_id)
        if chat is None:
            return {"channel": False, "service": False, "call": False, "saved": False}
        return {
            "channel": chat_is_channel(chat),
            "service": chat_is_service(chat) or self.is_service_dialog(chat),
            "call": chat_is_call(chat),
            "saved": self.is_saved_chat(chat),
        }

    async def label_for_chat(self, chat_id: int) -> str:
        """Подпись чата по его id (для статусов прочтения)."""
        chat = self._chats.get(chat_id)
        if chat is None:
            try:
                chat = await self.client.get_chat(chat_id)
            except Exception:
                log.debug("не смог получить чат %s", chat_id, exc_info=True)
                chat = None
            if chat is not None:
                self._chats[chat_id] = chat
        if chat is not None:
            try:
                _, label, _ = await self.chat_entry(chat)
                return label
            except Exception:
                log.debug("не смог собрать подпись чата %s", chat_id, exc_info=True)
        user = await self.get_user(chat_id)
        return user_name(user, chat_id)

    def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"})
        return self._http

    async def download_bytes(
        self,
        url: str,
        timeout: float = 30.0,
        max_bytes: int | None = None,
    ) -> bytes | None:
        try:
            async with self._session().get(
                url, timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                if response.status != 200:
                    log.debug("загрузка %s: HTTP %s", url, response.status)
                    return None
                if max_bytes and response.content_length and response.content_length > max_bytes:
                    log.warning(
                        "пропускаю %s: %d МБ больше лимита",
                        url,
                        response.content_length // (1024 * 1024),
                    )
                    return None
                return await response.read()
        except Exception:
            log.debug("не смог скачать %s", url, exc_info=True)
            return None

    async def video_url(self, attach: Any, candidates: list[tuple[int, Any]]) -> str | None:
        """Ссылка на видео: MAX отдаёт её только для пары (чат, видимое сообщение)."""
        for chat_id, message_id in candidates:
            try:
                info = await self.client.get_video_by_id(
                    chat_id=chat_id,
                    message_id=message_id,
                    video_id=attach.video_id,
                )
            except Exception:
                log.debug(
                    "нет видео %s в %s/%s", attach.video_id, chat_id, message_id, exc_info=True
                )
                continue
            if info is not None and info.url:
                return info.url
        return None

    async def file_url(self, attach: Any, candidates: list[tuple[int, Any]]) -> str | None:
        """Ссылка на файл: та же логика, что и у видео."""
        for chat_id, message_id in candidates:
            try:
                info = await self.client.get_file_by_id(
                    chat_id=chat_id,
                    message_id=message_id,
                    file_id=attach.file_id,
                )
            except Exception:
                log.debug(
                    "нет файла %s в %s/%s", attach.file_id, chat_id, message_id, exc_info=True
                )
                continue
            if info is not None and info.url:
                return info.url
        return None

    async def collect_media(
        self,
        message: Message,
        max_bytes: int = MAX_UPLOAD,
        want_voice: bool = True,
        want_stickers: bool = False,
    ) -> tuple[list[IncomingMedia], str]:
        """Скачивает фото/видео/файлы/голосовые (включая пересланные) и пометку о неудачах."""
        chain: list[Message] = []
        current: Message | None = message
        while current is not None and len(chain) < 3:
            chain.append(current)
            current = forwarded_message(current)

        # MAX ищет вложение по чату и id сообщения, где оно видно,
        # поэтому кандидаты идут от самого внешнего к источнику пересылки.
        candidates: list[tuple[int, Any]] = []
        for level in chain:
            if level.chat_id is None:
                continue
            pair = (level.chat_id, level.id)
            if pair not in candidates:
                candidates.append(pair)

        photos: dict[Any, Any] = {}
        videos: list[Any] = []
        files: list[Any] = []
        audios: list[Any] = []
        stickers: list[Any] = []

        for source in chain:
            for attach in source.attaches or []:
                kind = getattr(attach, "type", None)
                kind = getattr(kind, "value", kind)
                if kind == "PHOTO" and getattr(attach, "base_url", None):
                    key = getattr(attach, "photo_id", id(attach))
                    best = photos.get(key)
                    area = (attach.width or 0) * (attach.height or 0)
                    if best is None or area > (best.width or 0) * (best.height or 0):
                        photos[key] = attach
                elif kind == "VIDEO":
                    videos.append(attach)
                elif kind == "FILE":
                    files.append(attach)
                elif kind == "AUDIO" and getattr(attach, "url", None) and want_voice:
                    audios.append(attach)
                elif kind == "STICKER" and getattr(attach, "url", None) and want_stickers:
                    stickers.append(attach)

        result: list[IncomingMedia] = []
        failed: list[str] = []

        picked = list(photos.values())[:MAX_MEDIA]
        if picked:
            blobs = await asyncio.gather(*(self.download_bytes(p.base_url) for p in picked))
            for blob in blobs:
                if blob:
                    result.append(IncomingMedia("photo", "photo.jpg", blob))
                else:
                    failed.append("фото")

        for attach in audios[: max(0, MAX_MEDIA - len(result))]:
            blob = await self.download_bytes(
                attach.url, timeout=120, max_bytes=max_bytes
            )
            if blob:
                # MAX отдаёт голосовые как ogg/opus — по сигнатуре и решаем,
                # отправлять это голосовым сообщением или файлом
                name = "voice.ogg" if blob[:4] == b"OggS" else "audio.bin"
                result.append(IncomingMedia("voice", name, blob))
            else:
                failed.append("голосовое")

        for attach in stickers[: max(0, MAX_MEDIA - len(result))]:
            blob = await self.download_bytes(attach.url, timeout=60, max_bytes=max_bytes)
            if blob:
                name = "sticker.webp"
                if blob[:8].lower().startswith(b"gif8") or blob[:4] == b"RIFF":
                    name = "sticker.webp"
                elif blob[:4] == b"\x1f\x8b":
                    name = "sticker.tgs"
                result.append(IncomingMedia("sticker", name, blob))
            else:
                failed.append("стикер")

        for attach in videos[: max(0, MAX_MEDIA - len(result))]:
            url = await self.video_url(attach, candidates)
            blob = (
                await self.download_bytes(url, timeout=300, max_bytes=max_bytes)
                if url
                else None
            )
            if blob:
                result.append(IncomingMedia("video", "video.mp4", blob))
            else:
                failed.append("видео")

        for attach in files[: max(0, MAX_MEDIA - len(result))]:
            url = await self.file_url(attach, candidates)
            blob = (
                await self.download_bytes(url, timeout=180, max_bytes=max_bytes)
                if url
                else None
            )
            if blob:
                result.append(IncomingMedia("document", attach.name or "file", blob))
            else:
                failed.append(attach.name or "файл")

        note = ""
        if failed:
            unique = list(dict.fromkeys(failed))
            note = f"[не переслано: {', '.join(unique)}]"
        return result, note

    async def mark_read(self, chat_id: int, message_id: int) -> None:
        """Отмечает сообщение прочитанным в MAX."""
        try:
            await self.client.read_message(message_id=message_id, chat_id=chat_id)
        except Exception:
            log.debug("не смог отметить прочитанным %s/%s", chat_id, message_id, exc_info=True)

    async def avatar_bytes(self, url: str | None, timeout: float = 10.0) -> bytes | None:
        """Скачивает аватарку; MAX-CDN может быть недоступен для серверов Telegram."""
        if not url:
            return None
        if url in self._avatars:
            return self._avatars[url]
        data = await self.download_bytes(url, timeout=timeout)
        if data is None:
            self._avatars[url] = None
            return None
        data = await asyncio.to_thread(shrink_avatar, data)
        log.debug("аватарка %s: %d КБ", url, len(data) // 1024)
        self._avatars[url] = data
        return data

    async def aclose(self) -> None:
        if self._http is not None and not self._http.closed:
            await self._http.close()

    def profile_label(self) -> str:
        me = self.client.me
        if me is None or me.contact is None:
            return "MAX"
        return user_name(me.contact, me.contact.id)

    async def list_chats(self, limit: int = 12) -> list[tuple[int, str, str | None, int]]:
        """Чаты, куда можно писать (забаненных и служебные не показываем)."""
        candidates = [
            chat
            for chat in await self._sorted_chats()
            if not chat_is_readonly(chat)
            and not chat_is_stale_call(chat)
            and not chat_is_service(chat)
        ]
        # собеседников подтягиваем пачкой: иначе служебные аккаунты и баны не узнать
        await self._warm_users(candidates[: limit + 8])
        ordered = [
            chat
            for chat in candidates
            if not self.is_service_dialog(chat) and not self.peer_blocked(chat)
        ]
        return await self._entries(ordered[:limit])

    async def _entries(
        self, chats: Sequence[Chat]
    ) -> list[tuple[int, str, str | None, int]]:
        """Подписи для чатов, устойчиво к удалённым собеседникам."""
        results = await asyncio.gather(
            *(self.chat_entry(chat) for chat in chats), return_exceptions=True
        )
        entries: list[tuple[int, str, str | None, int]] = []
        for chat, result in zip(chats, results):
            when = chat.last_event_time or 0
            if isinstance(result, BaseException):
                log.debug("не смог собрать подпись чата %s", chat.id, exc_info=result)
                entries.append((chat.id, chat.title or f"чат {chat.id}", None, when))
                continue
            chat_id, label, avatar = result
            entries.append((chat_id, label, avatar, when))
        return entries

    async def _peers_of(self, chats: Sequence[Chat]) -> list[int]:
        peers: list[int] = []
        for chat in chats:
            if chat_kind_of(chat) != "DIALOG" or chat.title:
                continue
            for user_id in chat.participants or {}:
                if user_id != self.me_id and user_id not in peers:
                    peers.append(user_id)
        return peers

    def peer_blocked(self, chat: Chat) -> bool:
        """Забаненный или удалённый собеседник: MAX не даёт ему писать.

        Признаки: ``account_status`` не 0 (на живых данных 1 и 2) либо профиль
        вообще не отдаётся сервером.
        """
        if chat_kind_of(chat) != "DIALOG":
            return False
        peers = [uid for uid in (chat.participants or {}) if uid != self.me_id]
        if len(peers) != 1:
            return False
        peer = peers[0]
        if peer in self._missing_users:
            return True
        user = self._users.get(peer)
        if user is None:
            return False
        status = getattr(user, "account_status", None)
        return status not in (None, 0)

    async def _warm_users(self, chats: Sequence[Chat]) -> None:
        """Подтягивает незнакомых собеседников диалогов одним запросом."""
        peers = [uid for uid in await self._peers_of(chats) if uid not in self._users]
        if not peers:
            return
        try:
            users = await self.client.fetch_users(peers)
        except Exception:
            log.debug("не смог подтянуть пользователей для отсева", exc_info=True)
            return
        returned = set()
        for user in users or []:
            if user is not None:
                self._users[user.id] = user
                returned.add(user.id)
        # кого сервер не отдал — считаем недоступными (забанен/удалён)
        self._missing_users |= set(peers) - returned

    async def list_readonly_chats(self, limit: int = 200) -> list[tuple[int, str, str | None, int]]:
        """Чаты только для чтения: каналы и группы, где пишут только админы."""
        ordered = [
            chat
            for chat in await self._sorted_chats()
            if chat_is_readonly(chat) and not chat_is_service(chat)
        ]
        await self._warm_users(ordered[:limit])
        return await self._entries(ordered[:limit])

    async def _sorted_chats(self) -> list[Chat]:
        try:
            chats = await self.client.fetch_chats()
        except Exception:
            log.warning("fetch_chats не удался, беру кеш", exc_info=True)
            chats = list(self.client.chats or [])
        self._remember_chats(chats)
        return sorted(self._chats.values(), key=lambda c: c.last_event_time or 0, reverse=True)

    async def resolve_target(self, target: str) -> tuple[int, str]:
        """Приводит номер телефона или id чата к (chat_id, подпись)."""
        target = target.strip()
        if not target:
            raise ValueError("пустая цель")

        digits = target.lstrip("+")
        if not digits.isdigit():
            raise ValueError("нужен номер (+7...) или числовой id чата")

        if target.startswith("+") or len(digits) >= 10:
            user = await self.client.search_by_phone(target)
            if self.me_id is None:
                raise ValueError("MAX-клиент не знает свой id")
            chat_id = self.client.get_chat_id(
                first_user_id=self.me_id,
                second_user_id=user.id,
            )
            return chat_id, user_name(user, user.id)

        chat_id = int(digits)
        chat = self._chats.get(chat_id)
        if chat is None:
            try:
                chat = await self.client.get_chat(chat_id)
            except Exception:
                return chat_id, f"чат {chat_id}"
            self._chats[chat.id] = chat
        chat_id_, label, _ = await self.chat_entry(chat)
        return chat_id_, label

    # --- отправка -------------------------------------------------------

    async def send(
        self,
        chat_id: int,
        text: str | None = None,
        attachments: Sequence[Any] | None = None,
        reply_to: int | None = None,
    ) -> Message:
        await self.ensure_ready()
        return await self.client.send_message(
            chat_id=chat_id,
            text=text,
            attachments=attachments,
            reply_to=reply_to,
        )
