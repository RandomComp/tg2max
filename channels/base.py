"""Общий слой каналов: мессенджеры приводятся к одному виду.

Канал — адаптер над мессенджером. Он умеет:
  * отдавать события наружу в нейтральном виде (``Incoming`` / ``Change``);
  * отправлять в свой мессенджер текст и медиа;
  * выполнять действия, которые просит главный канал: поставить реакцию,
    отметить прочитанным, показать «печатает», изменить или удалить сообщение;
  * перечислять свои чаты (для единого списка ``/chats``).

Один канал назначается главным (``hub``): в нём владелец читает все сообщения и
отвечает на них. Остальные каналы (``spokes``) передают сообщения главному и
получают от него ответы и действия. Правила живут в ``bridge.py`` и о
внутренностях мессенджеров не знают — новый мессенджер добавляется одним
адаптером с этим интерфейсом.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Protocol, Sequence

# Виды вложений, к которым приводятся все мессенджеры
PHOTO = "photo"
VIDEO = "video"
VOICE = "voice"
DOCUMENT = "document"
MEDIA_KINDS = (PHOTO, VIDEO, VOICE, DOCUMENT)


@dataclass
class Media:
    """Вложение, готовое к отправке в другой мессенджер."""

    kind: str  # из MEDIA_KINDS
    name: str
    data: bytes


@dataclass
class ChatRef:
    """Чат мессенджера для единого списка /chats."""

    channel: str
    chat_id: int
    title: str
    avatar_url: str | None = None
    can_write: bool = True
    detail: str = ""
    ts: int = 0

    @property
    def address(self) -> str:
        """Адрес чата: ``max:367115027``."""
        return f"{self.channel}:{self.chat_id}"


@dataclass
class MenuItem:
    """Пункт меню: подпись и действие (или ссылка).

    Действие — строка, которую понимает ядро (``set:get_channels``,
    ``pick:max:367115027``, ``menu:settings:1``). Кнопочный канал отдаёт её
    в callback, текстовый — печатает список с номерами, и номер означает
    то же самое действие.
    """

    label: str
    action: str = ""
    url: str | None = None
    avatar_url: str | None = None
    group: str = ""
    title: str = ""
    detail: str = ""
    full_row: bool = False
    numbered: bool = False  # подпись уже с номером (текстовому меню номер не добавлять)


@dataclass
class Incoming:
    """Входящее сообщение в нейтральном виде."""

    channel: str
    chat_id: int
    message_id: int
    text: str = ""
    sender_id: int | None = None
    sender_name: str = ""
    ts: int = 0
    media: list[Media] = field(default_factory=list)
    attach_kinds: str = "text"
    note: str = ""
    reply_to: int | None = None
    raw: Any = None

    @property
    def media_kinds(self) -> str:
        if not self.media:
            return self.attach_kinds or "text"
        return ",".join(sorted({item.kind for item in self.media}))


@dataclass
class Change:
    """Событие, которое не является новым сообщением."""

    channel: str
    kind: str  # edit | delete | read | typing | reaction
    chat_id: int
    message_id: int = 0
    text: str = ""
    label: str = ""
    ts: int = 0
    mark: int = 0
    message_ids: list[int] = field(default_factory=list)
    reactions: list[tuple[str, int]] = field(default_factory=list)
    mine: str | None = None
    by_owner: bool = False
    ttl: bool = False
    raw: Any = None


EventHandler = Callable[[Incoming], Awaitable[None]]
ChangeHandler = Callable[[Change], Awaitable[None]]


class Channel(Protocol):
    """Что должен уметь канал мессенджера."""

    name: str
    display: str
    on_message: EventHandler | None
    on_change: ChangeHandler | None

    async def start(self) -> None:
        """Запустить приём событий."""

    async def stop(self) -> None:
        """Остановить канал и отпустить ресурсы."""

    async def send_text(
        self, chat_id: int, text: str, reply_to: int | None = None
    ) -> int | None:
        """Отправить текст, вернуть id отправленного сообщения."""

    async def send_media(
        self,
        chat_id: int,
        media: Sequence[Media],
        caption: str | None = None,
        reply_to: int | None = None,
    ) -> list[int]:
        """Отправить вложения, вернуть id отправленных сообщений."""

    async def show_typing(self, chat_id: int) -> None:
        """Показать, что в чате что-то набирают."""

    async def send_menu(
        self,
        chat_id: int,
        text: str,
        items: Sequence[MenuItem],
        edit_message_id: int | None = None,
    ) -> int | None:
        """Показать меню: кнопками, если канал умеет, иначе текстом с номерами."""

    async def react(self, chat_id: int, message_id: int, emoji: str | None) -> None:
        """Поставить (или снять, если ``None``) реакцию на сообщение."""

    async def mark_read(self, chat_id: int, message_id: int) -> None:
        """Отметить сообщение прочитанным."""

    async def edit_text(self, chat_id: int, message_id: int, text: str) -> None:
        """Изменить текст своего сообщения."""

    async def delete(self, chat_id: int, message_ids: Sequence[int]) -> None:
        """Удалить сообщения."""

    async def list_chats(self) -> list[ChatRef]:
        """Чаты этого мессенджера для единого списка."""

    async def chat_flags(self, chat_id: int) -> dict[str, bool]:
        """Особенности чата: канал, служебный, звонок, Избранное.

        Каналы, которым это не свойственно, возвращают пустой словарь.
        """
