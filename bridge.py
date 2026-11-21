"""Ядро моста: один главный канал, остальные — источники.

Что здесь происходит
--------------------
* Сообщение из любого канала-источника показывается в главном канале с
  подписью ``КАНАЛ · имя`` и запоминается маршрут «сообщение в главном →
  канал и чат источника».
* Ответ в главном канале (reply на такое сообщение или выбранный активный
  чат) уходит обратно в канал-источник.
* Главный канал может попросить источник: отправить текст или медиа,
  поставить реакцию, отметить прочитанным, показать «печатает», изменить
  или удалить сообщение.

Правила (кого игнорировать, тихие часы, история, лимиты) одинаковы для всех
мессенджеров: ядро знает только интерфейс ``Channel``.
"""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Sequence

from channels.base import (
    Change,
    Channel,
    ChatRef,
    Incoming,
    Media,
    MenuItem,
)
from settings import BY_KEY, LIMIT_PRESETS, OPTIONS, Option, Settings
from store import RouteRow, Store

log = logging.getLogger("bridge.core")

TYPING_THROTTLE = 5.0
TEXT_LIMIT = 3500
MENU_PAGE = 6
HISTORY_PAGE = 8
HISTORY_TOTAL_LIMIT = 5000  # сколько всего готовы вычитать из мессенджера
HISTORY_STREAMS_KEPT = 10  # сколько незакрытых генераторов держим в памяти

CHATS_VIEWS = ["short", "long"]

def history_preview(row) -> str:  # noqa: ANN001 - HistoryRow из store
    """Подпись пункта истории: текст, тип вложения или пометка об удалении."""
    preview = (row.text or "").replace("\n", " ").strip()
    if row.deleted:
        preview = f"[Удалено] {preview}".strip()
    elif not preview:
        if row.kind in {"text", "edit", ""}:
            preview = "(без текста)"
        else:
            preview = f"[{row.kind}]"
    return preview[:45] or "…"

HUB_HELP = (
    "Команды моста:\n"
    "/chats — список чатов-источников (потом можно выбрать номером)\n"
    "/to канал:чат текст — написать в конкретный чат\n"
    "/settings — список настроек с номерами\n"
    "/settings 5 выкл — изменить настройку по номеру\n"
    "/me — что подключено\n"
    "/stop — сбросить активный чат\n\n"
    "Ответь на сообщение — ответ уйдёт в тот же чат источника."
)


class BridgeCore:
    def __init__(
        self,
        channels: dict[str, Channel],
        store: Store,
        settings: Settings,
        inbox: dict[str, int],
    ) -> None:
        """``inbox`` — в какой чат канала складывать чужие сообщения."""
        self.channels = channels
        self.store = store
        self.settings = settings
        self.inbox = inbox
        self._typing_at: dict[tuple[str, int], float] = {}
        self._chat_lists: dict[int, list[ChatRef]] = {}
        self._menu_items: dict[int, list[MenuItem]] = {}
        self._menu_of_chat: dict[int, str] = {}
        self._menu_page: dict[int, int] = {}
        self._menu_of_message: dict[tuple[int, int], str] = {}
        self._last_menu: dict[int, int] = {}  # чат -> сообщение текущего меню
        self._pending_input: dict[int, str] = {}  # чат -> ключ настройки
        self._synced_at: dict[tuple[str, int], float] = {}
        self._synced_added: dict[tuple[str, int], int] = {}
        self._history_streams: dict[tuple[str, int], Any] = {}
        self._history_done: dict[tuple[str, int], bool] = {}
        for channel in channels.values():
            channel.on_message = self.on_message
            channel.on_change = self.on_change

    # --- вспомогательное -------------------------------------------------

    @property
    def hub_name(self) -> str:
        name = self.settings.text("hub_channel") or "telegram"
        return name if name in self.channels else next(iter(self.channels))

    @property
    def hub(self) -> Channel:
        return self.channels[self.hub_name]

    @property
    def clients(self) -> list[str]:
        return [self.channels[channel].display for channel in self.channels if channel != self.hub_name]

    def display(self, channel_name: str) -> str:
        channel = self.channels.get(channel_name)
        return getattr(channel, "display", channel_name.upper())

    def pair(self, channel_name: str, chat_id: int) -> str:
        """Подпись чата в едином списке: ``MAX · Лера``."""
        return f"{self.display(channel_name)} · {chat_id}"  # заменяется на название

    async def hub_inbox(self, channel_name: str | None = None) -> int:
        name = channel_name or self.hub_name
        return int(self.inbox.get(name, 0))

    def quiet_now(self) -> bool:
        if not self.settings.bool("quiet_hours"):
            return False
        return self.settings.quiet_now(time.localtime().tm_hour)

    def _apply_limits(self, channel_name: str) -> None:
        """Передаёт каналу текущие лимиты и настройку голосовых."""
        channel = self.channels[channel_name]
        if hasattr(channel, "limit_bytes"):
            channel.limit_bytes = max(1, self.settings.int("limit_to_tg")) * 1024 * 1024
        if hasattr(channel, "want_voice"):
            channel.want_voice = self.settings.bool("forward_voice")

    async def allowed(self, channel_name: str, chat_id: int) -> bool:
        """Пересылать ли сообщения из этого чата."""
        if await self.store.is_muted(channel_name, chat_id):
            return False
        flags = await self.channels[channel_name].chat_flags(chat_id)
        if not flags:
            return True
        if flags.get("channel") and not self.settings.bool("get_channels"):
            return False
        if flags.get("service") and not self.settings.bool("get_service"):
            return False
        if flags.get("call") and not self.settings.bool("get_calls"):
            return False
        if flags.get("saved") and not self.settings.bool("get_saved"):
            return False
        return True

    # --- единый список чатов --------------------------------------------

    async def all_chats(self) -> list[ChatRef]:
        """Чаты всех каналов, кроме самого главного."""
        hub = self.hub_name
        result: list[ChatRef] = []
        for name, channel in self.channels.items():
            if name == hub:
                continue
            try:
                result.extend(await channel.list_chats())
            except Exception:
                log.warning("не смог получить чаты канала %s", name, exc_info=True)
        return result

    async def find_chat(self, address: str) -> ChatRef | None:
        """Ищет чат по адресу ``max:367115027``."""
        channel_name, _, raw = address.partition(":")
        if not channel_name or not raw.lstrip("-").isdigit():
            return None
        chat_id = int(raw)
        if channel_name == self.hub_name:
            return ChatRef(self.hub_name, chat_id, address, can_write=True)
        for ref in await self.all_chats():
            if ref.channel == channel_name and ref.chat_id == chat_id:
                return ref
        return ChatRef(channel_name, chat_id, address, can_write=True)

    # --- приём из каналов -----------------------------------------------

    async def on_message(self, incoming: Incoming) -> None:
        await self._from_spoke(incoming)

    async def _from_spoke(self, incoming: Incoming) -> None:
        """Сообщение из источника: в историю, затем в главный канал."""
        self._apply_limits(incoming.channel)

        await self.store.add_history(
            incoming.channel,
            incoming.chat_id,
            incoming.message_id,
            incoming.sender_id,
            incoming.sender_name,
            incoming.ts,
            incoming.media_kinds,
            incoming.text or None,
        )

        if not await self.allowed(incoming.channel, incoming.chat_id):
            log.debug("чат %s:%s пропущен", incoming.channel, incoming.chat_id)
            return
        if self.quiet_now():
            log.debug("тихие часы — %s:%s не пересылаю", incoming.channel, incoming.chat_id)
            return

        hub_chat = await self.hub_inbox()
        head = f"{self.display(incoming.channel)} · {incoming.sender_name}"
        body = head if not incoming.text else f"{head}\n{incoming.text}"
        if incoming.note:
            body = f"{body}\n{incoming.note}".strip()

        log.warning(f"_from_spoke {incoming=}")

        sent_ids: list[int] = []
        if incoming.media:
            sent_ids = await self.hub.send_media(
                hub_chat, incoming.media, caption=body[:1000]
            )
        elif body:
            message_id = await self.hub.send_text(hub_chat, body)
            if message_id:
                sent_ids = [message_id]
        if not sent_ids:
            return

        await self.store.remember_route(
            self.hub_name,
            hub_chat,
            sent_ids[0],
            incoming.channel,
            incoming.chat_id,
            incoming.message_id,
            incoming.sender_name,
            incoming.ts,
        )
        await self.store.set_history_hub(
            incoming.channel,
            incoming.chat_id,
            incoming.message_id,
            self.hub_name,
            hub_chat,
            sent_ids[0],
        )

    async def _from_hub_user(self, incoming: Incoming) -> None:
        """Владелец написал в главном канале — отправляем в выбранный чат."""
        if await self._maybe_hub_command(incoming):
            return

        anchor = incoming.reply_to or incoming.message_id
        route = await self.store.route_for(self.hub_name, incoming.chat_id, anchor)
        target_channel = route.channel if route else None
        target_chat = route.chat_id if route else None
        title = route.title if route else ""
        reply_to = route.message_id if route else None

        if target_channel is None:
            active = await self.store.get_active(incoming.sender_id or 0)
            if active is None:
                return
            target_channel, target_chat, title = active

        await self.send_out(
            target_channel,
            target_chat,
            title,
            text=incoming.text,
            media=incoming.media,
            reply_to=reply_to,
            hub_chat_id=incoming.chat_id,
            hub_message_id=incoming.message_id,
        )

    # --- отправка из главного в источник --------------------------------

    async def send_out(
        self,
        channel_name: str,
        chat_id: int,
        title: str,
        *,
        text: str | None = None,
        media: Sequence[Media] | None = None,
        reply_to: int | None = None,
        hub_chat_id: int | None = None,
        hub_message_id: int | None = None,
    ) -> list[int]:
        """Отправляет сообщение в канал-источник и помнит маршрут."""
        channel = self.channels.get(channel_name)
        if channel is None:
            log.warning("нет канала %s", channel_name)
            return []

        body = (text or "").strip()
        sent_ids: list[int] = []
        if media:
            sent_ids = await channel.send_media(chat_id, media, caption=body[:1000] or None,
                                                reply_to=reply_to)
        elif body:
            message_id = await channel.send_text(chat_id, body[:TEXT_LIMIT], reply_to=reply_to)
            if message_id:
                sent_ids = [message_id]
        if not sent_ids:
            return []

        first = sent_ids[0]
        await self.store.record_own_message(
            channel_name,
            chat_id,
            first,
            hub_message_id,
            body or None,
            _media_kinds(media),
            int(time.time() * 1000),
            self.hub_name,
            hub_chat_id,
        )
        if hub_message_id is not None and hub_chat_id is not None:
            await self.store.remember_route(
                self.hub_name,
                hub_chat_id,
                hub_message_id,
                channel_name,
                chat_id,
                first,
                title,
                int(time.time() * 1000),
            )
        return sent_ids

    # --- подтягивание истории из мессенджера -----------------------------

    async def load_more_history(
        self, channel_name: str, chat_id: int, need: int
    ) -> int:
        """Догружает из мессенджера ещё ``need`` сообщений (по мере листания).

        Генератор канала живёт между вызовами, поэтому следующая страница
        продолжает то же чтение, а не запрашивает всё заново.
        """
        channel = self.channels.get(channel_name)
        loader = getattr(channel, "load_history", None)
        if loader is None or need <= 0:
            return 0

        key = (channel_name, chat_id)
        stream = self._history_streams.get(key)
        if stream is None:
            stream = loader(chat_id, HISTORY_TOTAL_LIMIT)
            self._history_streams[key] = stream
            if len(self._history_streams) > HISTORY_STREAMS_KEPT:
                oldest = next(iter(self._history_streams))
                if oldest != key:
                    self._history_streams.pop(oldest, None)

        added = 0
        while added < need:
            try:
                item = await stream.__anext__()
            except StopAsyncIteration:
                self._history_streams.pop(key, None)
                self._history_done[key] = True
                break
            except Exception:
                log.warning(
                    "не смог подтянуть историю %s:%s", channel_name, chat_id, exc_info=True
                )
                self._history_streams.pop(key, None)
                self._history_done[key] = True
                break

            if await self.store.has_history(channel_name, chat_id, item.message_id):
                continue
            await self.store.add_history(
                channel_name,
                chat_id,
                item.message_id,
                item.sender_id,
                item.sender_name,
                item.ts,
                item.media_kinds,
                item.text or None,
            )
            added += 1

        if added:
            self._synced_added[key] = self._synced_added.get(key, 0) + added
            log.info(
                "подтянул из %s историю чата %s: ещё %s сообщений",
                channel_name,
                chat_id,
                added,
            )
        return added

    # --- меню: один набор пунктов для всех каналов -----------------------

    async def menu(
        self, hub_chat_id: int, menu_id: str, page: int = 0, full: bool = False
    ) -> tuple[str, list[MenuItem]]:
        """Текст и пункты меню. Кнопочный канал рисует пункты кнопками,
        текстовый — нумерованным списком, и номер значит то же действие."""
        if menu_id.startswith("history:"):
            parts = menu_id.split(":")
            channel_name, raw_chat = parts[1], parts[2]
            cursor = parts[3] if len(parts) > 3 else "top"
            return await self._history_menu(channel_name, int(raw_chat), cursor)
        if menu_id == "hub":
            return await self._hub_menu()
        if menu_id.startswith("chats"):
            return await self._chats_menu(not menu_id.endswith("readonly"))
        if menu_id == "muted":
            return await self._muted_menu()
        return await self._settings_menu(page, full)

    async def show_menu(
        self,
        hub_chat_id: int,
        menu_id: str,
        page: int = 0,
        edit_message_id: int | None = None,
    ) -> int | None:
        """Показывает меню в главном канале."""
        self._menu_of_chat[hub_chat_id] = menu_id
        self._menu_page[hub_chat_id] = page
        try:
            return await self._show_menu(hub_chat_id, menu_id, page, edit_message_id)
        except Exception:
            log.warning(
                "не смог показать меню %s в канале %s", menu_id, self.hub_name, exc_info=True
            )
            return None

    async def _show_menu(
        self, hub_chat_id: int, menu_id: str, page: int, edit_message_id: int | None
    ) -> int | None:
        if getattr(self.hub, "supports_buttons", False):
            text, items = await self.menu(hub_chat_id, menu_id, page)
            message_id = await self.hub.send_menu(
                hub_chat_id, text, items, edit_message_id
            )
        else:
            text, items = await self.menu(hub_chat_id, menu_id, 0, full=True)
            number = 1
            lines = [text, ""]
            for item in items:
                detail = f" — {item.detail}" if item.detail else ""
                if item.numbered:
                    # подпись уже с номером — не дублируем
                    lines.append(f"{item.label}{detail}")
                else:
                    lines.append(f"{number}. {item.label}{detail}")
                number += 1
            lines.append("")
            lines.append("Напиши номер пункта. Настройку можно менять и так: /settings 16 200")
            self._menu_items[hub_chat_id] = items
            message_id = await self.hub.send_text(hub_chat_id, "\n".join(lines))
        if message_id:
            self._menu_of_message[(hub_chat_id, message_id)] = menu_id
            self._last_menu[hub_chat_id] = message_id
        return message_id

    async def refresh_menu(
        self, hub_chat_id: int, message_id: int | None = None, page: int | None = None
    ) -> None:
        """Перерисовывает меню (после нажатия кнопки или выбора номера).

        Правим сообщение только если это сообщение самого меню: карточку чата
        превращать в меню нельзя — Telegram не даёт менять текст у фото.
        """
        menu_id = None
        if message_id is not None:
            menu_id = self._menu_of_message.get((hub_chat_id, message_id))
        if menu_id is None:
            menu_id = self._menu_of_chat.get(hub_chat_id)
            # правим последнее сообщение меню, если оно относится к этому же меню
            last = self._last_menu.get(hub_chat_id)
            if last is not None and self._menu_of_message.get((hub_chat_id, last)) == menu_id:
                message_id = last
            else:
                message_id = None
        if menu_id is None:
            return
        page = self._menu_page.get(hub_chat_id, 0) if page is None else page
        await self.show_menu(hub_chat_id, menu_id, page, edit_message_id=message_id)

    async def _hub_menu(self) -> tuple[str, list[MenuItem]]:
        """Выбор главного канала кнопками (значений немного, вводить нечего)."""
        items = []
        for name in self.channels:
            mark = "✅ " if name == self.hub_name else ""
            items.append(
                MenuItem(label=f"{mark}{self.display(name)}", action=f"sethub:{name}")
            )
        items.append(MenuItem(label="◀ Назад", action="menu:settings:0"))
        text = (
            f"Главный канал: {self.display(self.hub_name)}.\n"
            "Выбери, где читать и отвечать: из остальных каналов сообщения "
            "будут приходить сюда."
        )
        return text, items

    async def _settings_menu(self, page: int, full: bool) -> tuple[str, list[MenuItem]]:
        options = list(OPTIONS)
        if full:
            chosen = options
            start = 0
        else:
            start = page * MENU_PAGE
            chosen = options[start : start + MENU_PAGE]
        pages = max(1, math.ceil(len(options) / MENU_PAGE))

        items: list[MenuItem] = []
        for offset, option in enumerate(chosen):
            number = start + offset + 1
            value = self._option_value(option)
            if option.kind == "bool":
                action = f"set:{option.key}"
            elif option.key == "hub_channel":
                action = "hubmenu"
            elif option.kind == "int":
                # значение неоднозначное — спросим его сообщением
                action = f"ask:{option.key}"
            else:
                action = f"setc:{option.key}"
            items.append(
                MenuItem(
                    label=f"{number}. {option.label}: {value}",
                    action=action,
                    detail=option.hint if not full else "",
                    numbered=True,
                )
            )

        text = (
            "Настройки: жми кнопку или пиши /settings <номер> <значение>."
            if not full
            else "Настройки: жми пункт номером или меняй командой /settings <номер> <значение>."
        )
        if not full and pages > 1:
            text += f" Страница {page + 1} из {pages}."

        # «Игнорируемые чаты» — отдельной строкой, листание — всегда в самом низу
        items.append(
            MenuItem(label="Игнорируемые чаты", action="menu:muted", full_row=True)
        )
        items.append(
            MenuItem(
                label="◀ Предыдущее",
                action=f"menu:settings:{(page - 1) % pages}",
            )
        )
        items.append(
            MenuItem(
                label="Дальше ▶",
                action=f"menu:settings:{(page + 1) % pages}",
            )
        )
        return text, items

    async def _chats_menu(self, can_write: bool) -> tuple[str, list[MenuItem]]:
        refs = [ref
            for ref in await self.all_chats()
            if ref.can_write == can_write and not await self.store.is_muted(ref.channel, ref.chat_id)
        ]
        # сортировка: сначала по каналу, затем по свежести последнего контакта
        refs.sort(key=lambda ref: (ref.channel, -(ref.ts or 0)))
        short = self.settings.text("chats_view") == "short"
        mode = "с аватарками" if not short else "короткий список"
        text = (
            "Чаты: что с ними сделать?" if can_write else "Чаты только для чтения:"
        )
        text += f"\nТекущий вид: {mode}. Переключается кнопкой ниже или /chats short|long."

        if not refs:
            text += "\nПока пусто."
            return text, [MenuItem(label="С аватарками" if short else "Короткий вид", action="chatsview")]

        text += f"\nНе показан(-ы) {await self.store.muted_chats_count()} чат(-ов), поскольку они были отмечены как игнорируемые"

        if short:
            # компактно: один чат в строку, свежие сверху; нажатие — писать туда
            # (для «только чтение» — открыть историю)
            items: list[MenuItem] = []
            for ref in refs[:30]:
                muted = await self.store.is_muted(ref.channel, ref.chat_id)

                if muted:
                    continue

                title = f"{self.display(ref.channel)} · {ref.title}"
                action = (
                    f"pick:{ref.channel}:{ref.chat_id}"
                    if ref.can_write
                    else f"hist:{ref.channel}:{ref.chat_id}:top"
                )
                items.append(MenuItem(label=title, action=action, full_row=True))
            items.append(MenuItem(label="С аватарками", action="chatsview"))
            return text, items

        items = []
        for ref in refs[:20]:
            muted = await self.store.is_muted(ref.channel, ref.chat_id)

            if muted:
                continue

            title = f"{self.display(ref.channel)} · {ref.title}"
            group = f"{ref.channel}:{ref.chat_id}"
            if ref.can_write:
                items.append(
                    MenuItem(
                        label="Писать",
                        action=f"pick:{group}",
                        group=group,
                        title=title,
                        avatar_url=ref.avatar_url,
                        detail=title,
                    )
                )
            items.append(
                MenuItem(
                    label="История",
                    action=f"hist:{group}:top",
                    group=group,
                    title=title,
                    avatar_url=ref.avatar_url,
                    detail=title,
                )
            )
            # items.append(
            #     MenuItem(
            #         label="Вернуть" if muted else "Игнорить",
            #         action=f"mute:{group}",
            #         group=group,
            #         title=title,
            #         avatar_url=ref.avatar_url,
            #         detail=title,
            #     )
            # )
        return text, items

    async def _history_page(
        self, channel_name: str, chat_id: int, cursor: str
    ) -> list:
        """Ровно одна страница истории вокруг курсора.

        ``top`` — самые свежие, ``n<id>`` — старше этой записи, ``p<id>`` —
        новее этой. Для «старше» догружаем из мессенджера, пока страница не
        наберётся или не кончится история.
        """
        key = (channel_name, chat_id)
        size = HISTORY_PAGE
        if cursor.startswith("p"):
            try:
                anchor = int(cursor[1:])
            except ValueError:
                return await self.store.history_newest(channel_name, chat_id, size)
            return await self.store.history_after(channel_name, chat_id, anchor, size)

        before: int | None = None
        if cursor.startswith("n"):
            try:
                before = int(cursor[1:])
            except ValueError:
                before = None

        rows = await self.store.history_before(channel_name, chat_id, before, size)
        tries = 0
        while (
            len(rows) < size
            and not self._history_done.get(key, False)
            and tries < 4
        ):
            tries += 1
            added = await self.load_more_history(channel_name, chat_id, size - len(rows))
            if not added:
                break
            rows = await self.store.history_before(channel_name, chat_id, before, size)
        return rows

    async def _history_menu(
        self, channel_name: str, chat_id: int, cursor: str = "top"
    ) -> tuple[str, list[MenuItem]]:
        key = (channel_name, chat_id)
        title = f"{self.display(channel_name)} · {await self.chat_title(channel_name, chat_id)}"

        rows = await self._history_page(channel_name, chat_id, cursor)
        if not rows and cursor != "top":
            # кнопка из старого сообщения или записи уже переехали — на начало
            cursor = "top"
            rows = await self._history_page(channel_name, chat_id, cursor)

        total = await self.store.history_count(channel_name, chat_id)
        if not rows:
            return f"История «{title}» пуста.", []

        first, last = rows[0], rows[-1]
        newer = await self.store.history_newer_count(channel_name, chat_id, first.message_id)
        page_no = newer // HISTORY_PAGE + 1
        older = await self.store.history_before(channel_name, chat_id, last.message_id, 1)
        source_more = (
            len(rows) >= HISTORY_PAGE and not self._history_done.get(key, False)
        )
        has_more = bool(older) or source_more
        log.info(
            "история %s:%s курсор %s, страница %s, в базе %s, отдаю записи %s",
            channel_name,
            chat_id,
            cursor,
            page_no,
            total,
            [row.row_id for row in rows],
        )
        # по одной записи в строку: сверху свежие, снизу старые, с автором
        items = [
            MenuItem(
                label=f"{row.label}: {history_preview(row)}",
                action=f"histshow:{row.row_id}",
                full_row=True,
            )
            for row in rows
        ]
        prev_cursor = f"p{first.message_id}" if newer else "top"
        next_cursor = f"n{last.message_id}" if has_more else "top"
        items.append(
            MenuItem(
                label="◀ Предыдущее",
                action=f"hist:{channel_name}:{chat_id}:{prev_cursor}",
            )
        )
        items.append(
            MenuItem(
                label="Дальше ▶",
                action=f"hist:{channel_name}:{chat_id}:{next_cursor}",
            )
        )
        if source_more:
            text = (
                f"История «{title}»: страница {page_no}, в базе {total} — "
                f"в мессенджере есть ещё, жми «Дальше»"
            )
        else:
            pages = max(page_no, math.ceil(total / HISTORY_PAGE))
            text = f"История «{title}»: страница {page_no} из {pages}, всего {total}"
        return text, items

    async def _muted_menu(self) -> tuple[str, list[MenuItem]]:
        rows = await self.store.muted_chats()
        items = [
            MenuItem(
                label=f"🔔 {self.display(channel_name)} · {title}",
                action=f"unmute:{channel_name}:{chat_id}",
            )
            for channel_name, chat_id, title in rows[:20]
        ]
        items.append(MenuItem(label="← Назад к настройкам", action="menu:settings:5"))
        text = (
            "Игнорируемые чаты: сообщения оттуда не идут в главный канал, "
            "но остаются в истории. Жми, чтобы вернуть."
            if rows
            else "Игнорируемых чатов нет. Игнорить можно из списка чатов."
        )
        return text, items

    async def apply_action(
        self, action: str, hub_chat_id: int, user_id: int
    ) -> str | None:
        """Выполняет действие из меню (кнопка или номер пункта)."""
        if not action or action == "noop":
            return None
        kind, _, rest = action.partition(":")

        if kind == "menu":
            if rest.startswith("settings"):
                _, _, raw_page = rest.partition(":")
                self._menu_of_chat[hub_chat_id] = "settings"
                self._menu_page[hub_chat_id] = int(raw_page) if raw_page.isdigit() else 0
            elif rest == "muted":
                self._menu_of_chat[hub_chat_id] = "muted"
                self._menu_page[hub_chat_id] = 0
            return None
        if kind == "set":
            option = BY_KEY.get(rest)
            if option is None:
                return None
            await self.settings.set(rest, not self.settings.bool(rest))
            return f"{option.label}: {self._option_value(option)}"
        if kind == "ask":
            # неоднозначная настройка: спрашиваем новое значение сообщением
            return await self.start_input(hub_chat_id, rest)
        if kind == "hubmenu":
            self._menu_of_chat[hub_chat_id] = "hub"
            self._menu_page[hub_chat_id] = 0
            return None
        if kind == "sethub":
            option = BY_KEY["hub_channel"]
            ok, value = self._coerce_value(option, rest)
            if not ok:
                return str(value)
            await self.settings.set(option.key, value)
            self._menu_of_chat[hub_chat_id] = "settings"
            self._menu_page[hub_chat_id] = 0
            return (
                f"{option.label}: {self._option_value(option)} — "
                "сообщения из остальных каналов теперь идут сюда."
            )
        if kind == "seti":
            option = BY_KEY.get(rest)
            if option is None:
                return None
            if option.key.startswith("limit_"):
                current = self.settings.int(rest)
                nxt = next((p for p in LIMIT_PRESETS if p > current), LIMIT_PRESETS[0])
                await self.settings.set(rest, nxt)
            elif option.key in {"quiet_from", "quiet_to"}:
                await self.settings.set(rest, (self.settings.int(rest) + 1) % 24)
            else:
                number = next(
                    (i for i, o in enumerate(OPTIONS, 1) if o.key == rest), 0
                )
                return (
                    f"{option.label}: {self.settings.int(rest)} "
                    f"(менять: /settings {number} <значение>)"
                )
            return f"{option.label}: {self._option_value(option)}"
        if kind == "setv":
            option = BY_KEY.get(rest)
            number = next((i for i, o in enumerate(OPTIONS, 1) if o.key == rest), 0)
            if option is None or not number:
                return None
            return await self.apply_setting(f"{number} {self._option_value(option)}")
        if kind == "setc":
            option = BY_KEY.get(rest)
            if option is None:
                return None
            values = self._cycle_values(option)
            if not values:
                return f"{option.label}: {self._option_value(option)}"
            current = self.settings.text(rest)
            index = values.index(current) if current in values else -1
            nxt = values[(index + 1) % len(values)]
            await self.settings.set(rest, nxt)
            return f"{option.label}: {self._option_value(option)}"
        if kind == "chatsview":
            views = list(CHATS_VIEWS)
            current = self.settings.text("chats_view")
            nxt = views[(views.index(current) + 1) % len(views)] if current in views else "long"
            await self.settings.set("chats_view", nxt)
            label = "с аватарками" if nxt == "long" else "короткий список"
            return f"Вид списка чатов: {label}"
        if kind == "seth":
            names = list(self.channels)
            current = names.index(self.hub_name) if self.hub_name in names else 0
            nxt = names[(current + 1) % len(names)]
            await self.settings.set("hub_channel", nxt)
            return f"Главный канал: {self.display(nxt)}"
        if kind == "hub":
            if rest in self.channels:
                await self.settings.set("hub_channel", rest)
                return f"Главный канал: {self.display(rest)}"
            return None
        if kind == "pick":
            ref = await self.find_chat(rest)
            channel_name, _, raw_chat = rest.partition(":")
            title = ref.title if ref else rest
            await self.store.set_active(user_id, channel_name, int(raw_chat), title)
            return f"Активный чат: {title}"
        if kind in {"mute", "unmute"}:
            channel_name, _, raw_chat = rest.partition(":")
            chat_id = int(raw_chat)
            ref = await self.find_chat(rest)
            title = ref.title if ref else rest
            muted = kind == "mute" and not await self.store.is_muted(channel_name, chat_id)
            await self.store.set_muted(channel_name, chat_id, title, muted)
            return (
                f"{title}: не пересылаю, пишу в историю"
                if muted
                else f"{title}: снова пересылаю"
            )
        if kind == "hist":
            _, channel_name, raw_chat, raw_cursor = action.split(":", 3)
            if not raw_cursor or raw_cursor.isdigit():
                raw_cursor = "top"  # старые кнопки со номером страницы
            self._menu_of_chat[hub_chat_id] = (
                f"history:{channel_name}:{raw_chat}:{raw_cursor}"
            )
            self._menu_page[hub_chat_id] = 0
            return None
        if kind == "histshow":
            row = await self.store.history_row(int(rest))
            if row is None:
                return None

            # 1) сообщение живо в главном канале — показываем копией
            copied = getattr(self.hub, "copy_message", None)
            if (
                copied is not None
                and row.hub_message_id
                and not row.deleted
                and row.hub_channel == self.hub_name
            ):
                if await copied(hub_chat_id, row.hub_message_id):
                    return None

            stamp = (
                time.strftime("%d.%m %H:%M", time.localtime(row.ts / 1000))
                if row.ts
                else ""
            )
            head = f"{self.display(row.channel)} · {row.label}"
            if stamp:
                head = f"{head} · {stamp}"

            # 2) тянем само сообщение из мессенджера — там есть вложения
            loader = getattr(self.channels.get(row.channel), "load_message", None)
            if loader is not None:
                item = await loader(row.chat_id, row.message_id)
                if item is not None and (item.media or item.text):
                    caption = f"{head}\n{item.text}".strip() if item.text else head
                    if item.note:
                        caption = f"{caption}\n{item.note}".strip()
                    if item.media:
                        await self.hub.send_media(hub_chat_id, item.media, caption=caption[:1000])
                    else:
                        await self.hub.send_text(hub_chat_id, f"{head}\n{item.text}")
                    return None

            # 3) запасной вариант — то, что лежит в истории
            body = row.text or ("[Удалено]" if row.deleted else f"[{row.kind}]")
            await self.hub.send_text(hub_chat_id, f"{head}\n{row.label}: {body}")
            return None
        return None

    # --- текстовые команды в главном канале ------------------------------

    async def _maybe_hub_command(self, incoming: Incoming) -> bool:
        """Команды и выбор номером в главном канале.

        Нужны там, где нет кнопок (например, MAX): та же логика, что у кнопочных
        каналов, только пункты выбираются номером.
        """
        text = (incoming.text or "").strip()
        if not text:
            return False
        hub_chat = incoming.chat_id
        user_id = incoming.sender_id or 0

        if text.isdigit():
            items = self._menu_items.get(hub_chat) or []
            index = int(text)
            if not 1 <= index <= len(items):
                return False
            action = items[index - 1].action
            result = await self.apply_action(action, hub_chat, user_id)
            if result:
                await self.hub.send_text(hub_chat, result)
            if action != "noop":
                await self.refresh_menu(hub_chat)
            return True

        if not text.startswith("/"):
            return False

        command, _, args = text.partition(" ")
        command = command.lower().lstrip("/")
        args = args.strip()

        if command in {"help", "помощь", "start"}:
            await self.hub.send_text(hub_chat, HUB_HELP)
            return True

        if command in {"chats", "чаты", "список"}:
            menu_id = "chats"
            mode = (args or "").split(" ")[0].lower()
            if mode in CHATS_VIEWS:
                await self.settings.set("chats_view", mode)
                await self.hub.send_text(
                    hub_chat,
                    f"Вид списка чатов: {self._option_value(BY_KEY['chats_view'])}",
                )
            elif mode.startswith("read") or mode == "ro":
                menu_id = "chats:readonly"
            await self.show_menu(hub_chat, menu_id)
            return True

        if command in {"settings", "настройки"}:
            if args:
                await self.hub.send_text(hub_chat, await self.apply_setting(args))
            await self.show_menu(hub_chat, "settings")
            return True

        if command in {"history", "история"}:
            target = None
            if incoming.reply_to:
                target = await self.reply_target(hub_chat, incoming.reply_to)
            if target is None:
                active = await self.store.get_active(user_id)
                if active is not None:
                    target = (active[0], active[1], active[2], None)
            if target is None:
                await self.hub.send_text(hub_chat, "Чат не выбран: /chats")
                return True
            await self.show_menu(hub_chat, f"history:{target[0]}:{target[1]}")
            return True

        if command in {"profile", "профиль"}:
            active = await self.store.get_active(user_id) if user_id else None
            if active is not None:
                channel_name, _chat_id, title = active

                await self.hub.send_text(hub_chat, f"Активный чат: {self.display(channel_name)} · {title}")
            return True

        if command in {"stop", "стоп"}:
            if user_id:
                await self.store.clear_active(user_id)
            await self.hub.send_text(hub_chat, "Активный чат сброшен.")
            return True

        if command in {"to", "в"}:
            target, _, rest = args.partition(" ")
            ref = await self.find_chat(target) if ":" in target else None
            if ref is None:
                await self.hub.send_text(
                    hub_chat, "Формат: /to канал:чат текст, например /to max:367115027"
                )
                return True
            if not rest.strip():
                if user_id:
                    await self.store.set_active(
                        user_id, ref.channel, ref.chat_id, ref.title
                    )
                await self.hub.send_text(hub_chat, f"Активный чат: {ref.title}")
                return True
            await self.send_out(ref.channel, ref.chat_id, ref.title, text=rest)
            await self.hub.send_text(hub_chat, f"Отправлено в {ref.title}")
            return True

        return False

    def _settings_summary(self) -> str:
        lines = ["Настройки (менять: /settings 5 выкл):"]
        for index, option in enumerate(OPTIONS, start=1):
            lines.append(f"{index}. {option.label}: {self._option_value(option)}")
        lines.append("В Telegram то же самое есть кнопками: /settings")
        return "\n".join(lines)

    def _option_value(self, option) -> str:  # noqa: ANN001 - Option из settings
        if option.kind == "bool":
            return "вкл" if self.settings.bool(option.key) else "выкл"
        if option.kind == "str":
            value = self.settings.text(option.key)
            if option.key == "hub_channel":
                return self.display(value) if value in self.channels else value
            if option.key == "chats_view":
                return "с аватарками" if value == "long" else "короткий список"
            return value
        return str(self.settings.int(option.key))

    def _cycle_values(self, option) -> list[str]:  # noqa: ANN001 - Option из settings
        """По каким значениям переключается строковая настройка."""
        if option.key == "hub_channel":
            return list(self.channels)
        if option.key == "chats_view":
            return list(CHATS_VIEWS)
        return []

    def option_value(self, key: str) -> str:
        """Текущее значение настройки текстом (для ответов пользователю)."""
        option = BY_KEY.get(key)
        return self._option_value(option) if option is not None else ""

    def _coerce_value(
        self, option: Option, raw: str
    ) -> tuple[bool, bool | int | str]:
        """Разбирает введённое значение настройки: ``(успех, значение или ошибка)``."""
        text = raw.strip().lower()
        if option.kind == "bool":
            if text in {"вкл", "включить", "да", "1", "on", "true", "yes"}:
                return True, True
            if text in {"выкл", "выключить", "нет", "0", "off", "false", "no"}:
                return True, False
            return False, f"Для «{option.label}» нужно вкл или выкл."
        if option.kind == "int":
            try:
                number = int(text)
            except ValueError:
                return False, f"Для «{option.label}» нужно число."
            if option.key in {"quiet_from", "quiet_to"} and not 0 <= number <= 23:
                return False, "Часы — от 0 до 23."
            if option.key.startswith("limit_") and not 1 <= number <= 4096:
                return False, "Лимит — от 1 до 4096 МБ."
            if option.key == "max_inbox" and number < 0:
                return False, "Номер чата — 0 (Избранное) или положительное число."
            return True, number
        if option.key == "chats_view":
            if text.lower() not in ["short", "long"]:
                return False, "Для вида списка: short или long."
            return True, text
        found = next(
            (
                name
                for name in self.channels
                if name == text or self.display(name).lower() == text
            ),
            None,
        )
        if found is None:
            names = ", ".join(
                f"{index}. {self.display(name)}"
                for index, name in enumerate(self.channels, start=1)
            )
            return False, f"Канал не понял. Доступные: {names}"
        return True, found

    def _input_hint(self, option: Option) -> str:
        if option.key in {"quiet_from", "quiet_to"}:
            return " (час, 0–23)"
        if option.key.startswith("limit_"):
            return " (1–4096 МБ)"
        if option.key == "max_inbox":
            return " (0 — Избранное, иначе id чата)"
        return ""

    def pending_input(self, hub_chat_id: int) -> str | None:
        """Ждём ли от этого чата значение настройки (ключ настройки)."""
        return self._pending_input.get(hub_chat_id)

    async def start_input(self, hub_chat_id: int, key: str) -> str:
        """Переходит в режим ввода нового значения: следующее сообщение — значение."""
        option = BY_KEY.get(key)
        if option is None:
            return "Не знаю такую настройку."
        self._pending_input[hub_chat_id] = key
        return (
            f"{option.label}: сейчас {self._option_value(option)}.\n"
            f"Пришли новое значение{self._input_hint(option)} сообщением. "
            "Отмена — /cancel."
        )

    async def apply_setting(self, spec: str) -> str:
        """``/settings 5 выкл`` — применить настройку и вернуть ответ.

        Без номера возвращает список; без значения булеву настройку
        переключает, а для числа или канала показывает текущее значение.
        """
        parts = spec.strip().split(maxsplit=1)
        if not parts or not parts[0].isdigit():
            return self._settings_summary()

        number = int(parts[0])
        if not 1 <= number <= len(OPTIONS):
            return f"Нет настройки №{number}. /settings — список."

        option = OPTIONS[number - 1]
        if len(parts) == 1:
            if option.kind == "bool":
                await self.settings.set(option.key, not self.settings.bool(option.key))
            return f"{option.label}: {self._option_value(option)}"

        raw = parts[1].strip().lower()
        ok, value = self._coerce_value(option, raw)
        if not ok:
            return str(value)

        await self.settings.set(option.key, value)
        return f"{option.label}: {self._option_value(option)}"

    async def reply_target(
        self, hub_chat_id: int, hub_message_id: int
    ) -> tuple[str, int, str, int | None] | None:
        """Куда отвечать на сообщение, показанное в главном канале."""
        route = await self.store.route_for(self.hub_name, hub_chat_id, hub_message_id)
        if route is None:
            return None
        return route.channel, route.chat_id, route.title, route.message_id

    async def hub_target(
        self, hub_chat_id: int, reply_to: int | None, user_id: int
    ) -> tuple[str, int, str, int | None] | None:
        """Куда уйдёт сообщение владельца: чат из reply или активный чат."""
        if reply_to:
            found = await self.reply_target(hub_chat_id, reply_to)
            if found is not None:
                return found
        active = await self.store.get_active(user_id)
        if active is None:
            return None
        channel_name, chat_id, title = active
        return channel_name, chat_id, title, None

    async def chat_title(self, channel_name: str, chat_id: int) -> str:
        """Название чата для заголовков истории."""
        if channel_name == self.hub_name:
            active = None
            return str(chat_id)
        for ref in await self.all_chats():
            if ref.channel == channel_name and ref.chat_id == chat_id:
                return ref.title
        return str(chat_id)

    # --- изменения (правки, удаления, прочтения, печатает, реакции) ------

    async def on_change(self, change: Change) -> None:
        channel = self.channels.get(change.channel)
        if channel is None:
            return

        if change.kind == "edit":
            await self._on_edit(change)
        elif change.kind == "delete":
            await self._on_delete(change)
        elif change.kind == "read":
            await self._on_read(change)
        elif change.kind == "typing":
            await self._on_typing(change)
        elif change.kind == "reaction":
            await self._on_reaction(change)

    async def _hub_notice(
        self,
        chat_id: int,
        text: str,
        reply_to: int | None = None,
    ) -> int | None:
        """Короткое уведомление в главном канале."""
        return await self.hub.send_text(chat_id, text, reply_to=reply_to)

    async def _run_upstream(self, channel_name: str, chat_id: int) -> bool:
        """Проходит ли чат фильтры: мьют, настройки, тихие часы."""
        if not await self.allowed(channel_name, chat_id):
            return False
        return not self.quiet_now()

    async def _on_edit(self, change: Change) -> None:
        await self.store.mark_history_deleted(
            change.channel, change.chat_id, change.message_id, only_latest=True
        )
        await self.store.add_history(
            change.channel,
            change.chat_id,
            change.message_id,
            None,
            change.label,
            change.ts,
            "edit",
            change.text or None,
        )
        if not self.settings.bool("notify_edit"):
            return
        if not await self._run_upstream(change.channel, change.chat_id):
            return

        hub_chat = await self.hub_inbox()
        routes = await self.store.routes_to_source(
            change.channel, change.chat_id, change.message_id
        )
        head = f"{self.display(change.channel)} · {change.label}"
        text = f"{head}\n[Изменено] {change.text}".strip()
        reply_to = routes[0].hub_message_id if routes else None
        sent = await self._hub_notice(hub_chat, text, reply_to)
        if sent and routes:
            await self.store.set_history_hub(
                change.channel,
                change.chat_id,
                change.message_id,
                self.hub_name,
                hub_chat,
                sent,
            )

    async def _on_delete(self, change: Change) -> None:
        for message_id in change.message_ids:
            await self.store.mark_history_deleted(
                change.channel, change.chat_id, message_id
            )
        if change.ttl or not self.settings.bool("notify_delete"):
            return
        if not await self._run_upstream(change.channel, change.chat_id):
            return

        hub_chat = await self.hub_inbox()
        for message_id in change.message_ids:
            routes = await self.store.routes_to_source(
                change.channel, change.chat_id, message_id
            )
            title = routes[0].title if routes else change.label
            head = f"{self.display(change.channel)} · {title or change.chat_id}"
            reply_to = routes[0].hub_message_id if routes else None
            await self._hub_notice(hub_chat, f"{head}\n[Удалено]", reply_to)

    async def _on_read(self, change: Change) -> None:
        if change.by_owner:
            # владелец сам читал в этом канале: если это инбокс главного —
            # сообщаем источникам, что их сообщения прочитаны
            if change.channel == self.hub_name and change.chat_id == await self.hub_inbox():
                await self._propagate_read_to_sources(change.chat_id)
            return

        rows = await self.store.unread_outgoing(
            change.channel, change.chat_id, change.mark
        )
        if not rows:
            return

        log.debug(f"{rows=}")

        await self.store.mark_read(
            change.channel,
            change.chat_id,
            [row.message_id for row in rows if row.message_id],
        )
        if not self.settings.bool("notify_read") or not await self._run_upstream(
            change.channel, change.chat_id
        ):
            return

        hub_chat = await self.hub_inbox()
        title = rows[0].title
        head = f"{self.display(change.channel)} · {title}"
        reply_to = rows[0].hub_message_id if len(rows) == 1 else None
        await self.hub.send_text(hub_chat, f"{head}\n[Прочитано]", reply_to=reply_to)

    async def _on_typing(self, change: Change) -> None:
        if not self.settings.bool("notify_typing"):
            return
        if not await self._run_upstream(change.channel, change.chat_id):
            return
        key = (change.channel, change.chat_id)
        now = time.monotonic()
        if now - self._typing_at.get(key, 0.0) < TYPING_THROTTLE:
            return
        self._typing_at[key] = now
        await self.hub.show_typing(await self.hub_inbox())

    async def _on_reaction(self, change: Change) -> None:
        if not self.settings.bool("notify_reactions"):
            return
        if not await self._run_upstream(change.channel, change.chat_id):
            return

        hub_chat = await self.hub_inbox()
        routes = await self.store.routes_to_source(
            change.channel, change.chat_id, change.message_id
        )
        marks = " ".join(f"{emoji}×{count}" for emoji, count in change.reactions if count)
        mine = f"твоя: {change.mine}" if change.mine else ""
        parts = [part for part in (marks, mine) if part]
        if not parts:
            return

        title = routes[0].title if routes else str(change.chat_id)
        head = f"{self.display(change.channel)} · {title}"
        for route in routes:
            emoji = change.mine or (marks.split("×")[0] if marks else None)
            await self.hub.react(route.hub_chat_id, route.hub_message_id, emoji)
            await self.hub.send_text(
                route.hub_chat_id,
                f"{head}\n[Реакция] {' '.join(parts)}",
                reply_to=route.hub_message_id,
            )
        if not routes:
            await self.hub.send_text(hub_chat, f"{head}\n[Реакция] {' '.join(parts)}")

    # --- действия главного канала в сторону источника --------------------

    async def react_to_source(
        self,
        channel_name: str,
        chat_id: int,
        message_id: int,
        emoji: str | None,
    ) -> None:
        channel = self.channels.get(channel_name)
        if channel is not None:
            await channel.react(chat_id, message_id, emoji)

    async def mark_read_in_source(self, channel_name: str, chat_id: int) -> None:
        last = await self.store.last_message_id(channel_name, chat_id)
        channel = self.channels.get(channel_name)
        if channel is not None and last:
            await channel.mark_read(chat_id, last)

    async def _propagate_read_to_sources(self, hub_chat_id: int) -> None:
        """Владелец прочитал пересланное в главном — отмечаем прочитанным в источниках."""
        for route in await self.store.routes_into_hub(self.hub_name, hub_chat_id):
            if not route.message_id:
                continue
            channel = self.channels.get(route.channel)
            if channel is not None:
                await channel.mark_read(route.chat_id, route.message_id)

    async def edit_in_source(
        self, channel_name: str, chat_id: int, message_id: int, text: str
    ) -> None:
        channel = self.channels.get(channel_name)
        if channel is not None:
            await channel.edit_text(chat_id, message_id, text)

    async def delete_in_source(
        self, channel_name: str, chat_id: int, message_ids: Sequence[int]
    ) -> None:
        channel = self.channels.get(channel_name)
        if channel is not None:
            await channel.delete(chat_id, message_ids)

    async def typing_in_source(self, channel_name: str, chat_id: int) -> None:
        channel = self.channels.get(channel_name)
        if channel is not None:
            await channel.show_typing(chat_id)


def _media_kinds(media: Sequence[Media] | None) -> str:
    if not media:
        return "text"
    return ",".join(sorted({item.kind for item in media}))
