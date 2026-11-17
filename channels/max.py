"""Канал MAX: обёртка над MaxBridge, приводящая его к общему интерфейсу."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any, Sequence

from pymax import File, Message, Photo, Video, Voice

from max_side import (
    MAX_UPLOAD,
    MaxBridge,
    attachment_note,
    chat_is_readonly,
    forwarded_message,
)

from .base import (
    DOCUMENT,
    PHOTO,
    VIDEO,
    VOICE,
    Change,
    ChatRef,
    Incoming,
    Media,
)

log = logging.getLogger(__name__)


class MaxChannel:
    """MAX как канал: события наружу, действия внутрь."""

    name = "max"
    display = "MAX"

    def __init__(
        self,
        bridge: MaxBridge,
        *,
        on_message=None,
        on_change=None,
        limit_bytes: int = MAX_UPLOAD,
        want_voice: bool = True,
    ) -> None:
        self.bridge = bridge
        self.on_message = on_message
        self.on_change = on_change
        self.limit_bytes = limit_bytes
        self.want_voice = want_voice
        self._task: asyncio.Task[Any] | None = None
        self._wire()

    # --- жизненный цикл -------------------------------------------------

    def _wire(self) -> None:
        self.bridge.on_incoming = self._relay_message
        self.bridge.on_edit = self._relay_edit
        self.bridge.on_delete = self._relay_delete
        self.bridge.on_read = self._relay_read
        self.bridge.on_typing = self._relay_typing
        self.bridge.on_reaction = self._relay_reaction

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self.bridge.run(), name="max-channel")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        await self.bridge.aclose()

    async def ensure_ready(self, timeout: float = 20.0) -> None:
        await self.bridge.ensure_ready(timeout)

    # --- события MAX -> нейтральный вид ---------------------------------

    async def _label(self, message: Message) -> tuple[str, str | None]:
        label, avatar = await self.bridge.label_for(message)
        origin = forwarded_message(message)
        if origin is not None:
            origin_label, origin_avatar = await self.bridge.label_for(origin)
            label = f"{origin_label} · переслано"
            avatar = origin_avatar or avatar
        return label, avatar

    async def _relay_message(self, message: Message, _client: Any = None) -> None:
        if self.on_message is None:
            return
        label, _avatar = await self._label(message)
        media, note = await self.bridge.collect_media(
            message, max_bytes=self.limit_bytes, want_voice=self.want_voice
        )
        incoming = Incoming(
            channel=self.name,
            chat_id=message.chat_id or 0,
            message_id=message.id,
            text=(message.text or "").strip(),
            sender_id=message.sender,
            sender_name=label,
            ts=message.time or 0,
            media=[Media(item.kind, item.name, item.data) for item in media],
            attach_kinds=_attach_kinds(message),
            note=" ".join(part for part in (attachment_note(message), note) if part),
            own=bool(self.bridge.me_id and message.sender == self.bridge.me_id),
            reply_to=_reply_id(message),
            raw=message,
        )
        await self.on_message(incoming)

    async def _relay_edit(self, message: Message, _client: Any = None) -> None:
        if self.on_change is None:
            return
        label, _avatar = await self._label(message)
        await self.on_change(
            Change(
                channel=self.name,
                kind="edit",
                chat_id=message.chat_id or 0,
                message_id=message.id,
                text=(message.text or "").strip(),
                label=label,
                ts=message.time or 0,
                raw=message,
            )
        )

    async def _relay_delete(self, event: Any, _client: Any = None) -> None:
        if self.on_change is None:
            return
        await self.on_change(
            Change(
                channel=self.name,
                kind="delete",
                chat_id=getattr(event, "chat_id", 0) or 0,
                message_ids=list(getattr(event, "message_ids", None) or []),
                ttl=bool(getattr(event, "ttl", False)),
                raw=event,
            )
        )

    async def _relay_read(self, event: Any) -> None:
        if self.on_change is None:
            return
        reader = getattr(event, "user_id", None)
        await self.on_change(
            Change(
                channel=self.name,
                kind="read",
                chat_id=getattr(event, "chat_id", 0) or 0,
                mark=int(getattr(event, "mark", 0) or 0),
                by_owner=bool(self.bridge.me_id and reader == self.bridge.me_id),
                raw=event,
            )
        )

    async def _relay_typing(self, event: Any) -> None:
        if self.on_change is None:
            return
        await self.on_change(
            Change(
                channel=self.name,
                kind="typing",
                chat_id=getattr(event, "chat_id", 0) or 0,
                raw=event,
            )
        )

    async def _relay_reaction(self, event: Any) -> None:
        if self.on_change is None:
            return
        counters = getattr(event, "counters", None) or []
        await self.on_change(
            Change(
                channel=self.name,
                kind="reaction",
                chat_id=getattr(event, "chat_id", 0) or 0,
                message_id=int(getattr(event, "message_id", 0) or 0),
                reactions=[
                    (
                        str(getattr(counter, "reaction", "")),
                        int(getattr(counter, "count", 0) or 0),
                    )
                    for counter in counters
                ],
                mine=getattr(event, "your_reaction", None),
                raw=event,
            )
        )

    # --- действия нейтрального вида -> MAX ------------------------------

    async def send_text(
        self, chat_id: int, text: str, reply_to: int | None = None
    ) -> int | None:
        sent = await self.bridge.send(chat_id, text, reply_to=reply_to)
        return getattr(sent, "id", None)

    async def send_media(
        self,
        chat_id: int,
        media: Sequence[Media],
        caption: str | None = None,
        reply_to: int | None = None,
    ) -> list[int]:
        attachments = [_to_pymax(item) for item in media]
        if not attachments:
            message_id = await self.send_text(chat_id, caption or "", reply_to=reply_to)
            return [message_id] if message_id else []
        sent = await self.bridge.send(
            chat_id, caption, attachments=attachments, reply_to=reply_to
        )
        message_id = getattr(sent, "id", None)
        return [message_id] if message_id else []

    async def show_typing(self, chat_id: int) -> None:
        # PyMax (2.5.0) не умеет отправлять MSG_TYPING — просто пропускаем
        log.debug("MAX: «печатает» не поддерживается библиотекой")

    async def react(self, chat_id: int, message_id: int, emoji: str | None) -> None:
        try:
            if emoji:
                await self.bridge.client.add_reaction(
                    chat_id=chat_id, message_id=message_id, reaction=emoji
                )
            else:
                await self.bridge.client.remove_reaction(
                    chat_id=chat_id, message_id=message_id
                )
        except Exception:
            log.debug("MAX: не смог поставить реакцию", exc_info=True)

    async def mark_read(self, chat_id: int, message_id: int) -> None:
        if message_id:
            await self.bridge.mark_read(chat_id, message_id)

    async def edit_text(self, chat_id: int, message_id: int, text: str) -> None:
        try:
            await self.bridge.client.edit_message(
                chat_id=chat_id, message_id=message_id, text=text
            )
        except Exception:
            log.debug("MAX: не смог изменить сообщение", exc_info=True)

    async def delete(self, chat_id: int, message_ids: Sequence[int]) -> None:
        if not message_ids:
            return
        try:
            await self.bridge.client.delete_message(
                chat_id=chat_id, message_ids=list(message_ids), for_me=False
            )
        except Exception:
            log.debug("MAX: не смог удалить сообщение", exc_info=True)

    async def list_chats(self) -> list[ChatRef]:
        refs: list[ChatRef] = []
        for chat_id, title, avatar, ts in await self.bridge.list_chats(limit=50):
            refs.append(ChatRef(self.name, chat_id, title, avatar, can_write=True, ts=ts))
        for chat_id, title, avatar, ts in await self.bridge.list_readonly_chats(limit=200):
            refs.append(
                ChatRef(
                    self.name,
                    chat_id,
                    title,
                    avatar,
                    can_write=False,
                    detail="только чтение",
                    ts=ts,
                )
            )
        return refs

    async def chat_flags(self, chat_id: int) -> dict[str, bool]:
        return await self.bridge.chat_flags(chat_id)

    async def resolve(self, target: str) -> tuple[int, str]:
        """Номер телефона или id чата -> (chat_id, подпись)."""
        return await self.bridge.resolve_target(target)

    async def load_history(
        self, chat_id: int, limit: int = 200, chunk: int = 50
    ) -> AsyncIterator[Incoming]:
        """История чата из самого MAX — генератором, страницами по ``chunk``.

        Отдаёт сообщения от свежих к старым, включая то, что было до запуска
        моста. Потребитель может остановиться в любой момент — лишнего не
        запрашивается.
        """
        chat = await self.bridge.get_chat(chat_id)
        if chat is None:
            return

        label: str | None = None
        loaded = 0
        before: int | None = None
        while loaded < limit:
            want = min(chunk, limit - loaded)
            try:
                page = await chat.history(backward=want, from_time=before)
            except Exception:
                log.debug("не смог получить историю чата %s", chat_id, exc_info=True)
                return
            if not page:
                return

            if label is None:
                label = await self._peer_label(chat)

            oldest: int | None = None
            for message in page:
                own = bool(self.bridge.me_id and message.sender == self.bridge.me_id)
                if message.time and (oldest is None or message.time < oldest):
                    oldest = message.time
                yield Incoming(
                    channel=self.name,
                    chat_id=chat_id,
                    message_id=message.id,
                    text=(message.text or "").strip(),
                    sender_id=message.sender,
                    sender_name="Я" if own else label,
                    ts=message.time or 0,
                    attach_kinds=_attach_kinds(message),
                    own=own,
                    raw=message,
                )

            loaded += len(page)
            if len(page) < want or not oldest:
                return
            before = oldest

    async def _peer_label(self, chat) -> str:  # noqa: ANN001 - Chat из pymax
        try:
            _chat_id, label, _avatar = await self.bridge.chat_entry(chat)
            return label
        except Exception:
            return chat.title or f"чат {chat.id}"

    async def load_message(self, chat_id: int, message_id: int) -> Incoming | None:
        """Одно сообщение из MAX вместе с вложениями — для показа из истории."""
        try:
            messages = await self.bridge.client.get_messages(chat_id, [message_id])
        except Exception:
            log.debug("не смог получить сообщение %s/%s", chat_id, message_id, exc_info=True)
            return None
        if not messages:
            return None

        message = messages[0]
        own = bool(self.bridge.me_id and message.sender == self.bridge.me_id)
        label = "Я" if own else await self._peer_label(await self.bridge.get_chat(chat_id))
        media, note = await self.bridge.collect_media(
            message,
            max_bytes=self.limit_bytes,
            want_voice=self.want_voice,
            want_stickers=True,
        )
        return Incoming(
            channel=self.name,
            chat_id=chat_id,
            message_id=message.id,
            text=(message.text or "").strip(),
            sender_id=message.sender,
            sender_name=label,
            ts=message.time or 0,
            media=[Media(item.kind, item.name, item.data) for item in media],
            attach_kinds=_attach_kinds(message),
            note=note,
            own=own,
            raw=message,
        )

    async def avatar_bytes(self, url: str | None) -> bytes | None:
        return await self.bridge.avatar_bytes(url)


def _reply_id(message: Message) -> int | None:
    """id сообщения, на которое ответили (ReplyLink), если это ответ."""
    link = getattr(message, "link", None)
    if link is None:
        return None
    kind = getattr(link, "type", None)
    kind = getattr(kind, "value", kind)
    if str(kind) != "REPLY":
        return None
    inner = getattr(link, "message", None)
    return int(getattr(inner, "id", 0) or 0) or None


def _attach_kinds(message: Message) -> str:
    """Тип вложений без скачивания — для подписи в истории."""
    mapping = {
        "PHOTO": PHOTO,
        "VIDEO": VIDEO,
        "FILE": DOCUMENT,
        "AUDIO": VOICE,
        "STICKER": "sticker",
        "CONTACT": "contact",
        "POLL": "poll",
        "CALL": "call",
        "SHARE": "forward",
        "UNKNOWN": "attachment",
    }
    kinds: list[str] = []
    for attach in message.attaches or []:
        kind = getattr(attach, "type", None)
        kind = getattr(kind, "value", kind)
        label = mapping.get(str(kind))
        if label and label not in kinds:
            kinds.append(label)
    return ",".join(kinds) or "text"


def _to_pymax(item: Media):
    """Нейтральное вложение -> объект PyMax для отправки."""
    if item.kind == PHOTO:
        return Photo(raw=item.data, name=item.name or "photo.jpg")
    if item.kind == VIDEO:
        return Video(raw=item.data, name=item.name or "video.mp4")
    if item.kind == VOICE:
        return Voice(raw=item.data, name=item.name or "voice.ogg")
    return File(raw=item.data, name=item.name or "file")
