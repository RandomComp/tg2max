"""Состояние моста: владелец, маршруты сообщений, история и настройки.

Всё в sqlite. Адресация общая для всех мессенджеров: чат — это пара
``(канал, chat_id)``, сообщение — ``(канал, chat_id, message_id)``.
Маршрут связывает сообщение в главном канале с сообщением в канале-источнике,
чтобы ответ и уведомления уходили туда, откуда пришло сообщение.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import aiosqlite

@dataclass
class HistoryRow:
    """Строка истории сообщений чата."""

    row_id: int
    channel: str
    chat_id: int
    message_id: int
    label: str
    kind: str
    text: str
    hub_channel: str | None
    hub_chat_id: int | None
    hub_message_id: int | None
    ts: int
    deleted: bool


@dataclass
class RouteRow:
    """Связка «сообщение в главном канале → сообщение в канале-источнике»."""

    hub_channel: str
    hub_chat_id: int
    hub_message_id: int
    channel: str
    chat_id: int
    message_id: int | None
    title: str
    ts: int


SCHEMA = """
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS active_chat (
    user_id INTEGER PRIMARY KEY,
    channel TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    updated REAL NOT NULL DEFAULT (strftime('%s', 'now'))
);

CREATE TABLE IF NOT EXISTS routes (
    hub_channel TEXT NOT NULL,
    hub_chat_id INTEGER NOT NULL,
    hub_message_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER,
    title TEXT NOT NULL,
    ts INTEGER,
    read INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (hub_channel, hub_chat_id, hub_message_id)
);

CREATE INDEX IF NOT EXISTS routes_source ON routes(channel, chat_id, ts);

CREATE TABLE IF NOT EXISTS muted (
    channel TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    muted INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (channel, chat_id)
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    sender INTEGER,
    label TEXT NOT NULL,
    ts INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL DEFAULT 'text',
    text TEXT,
    hub_channel TEXT,
    hub_chat_id INTEGER,
    hub_message_id INTEGER,
    forwarded INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    UNIQUE(channel, chat_id, message_id, version)
);

CREATE INDEX IF NOT EXISTS messages_chat_ts ON messages(channel, chat_id, ts DESC);

CREATE TABLE IF NOT EXISTS read_mark (
    hub_channel TEXT NOT NULL,
    hub_chat_id INTEGER NOT NULL,
    hub_message_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    PRIMARY KEY (hub_channel, hub_chat_id)
);

CREATE TABLE IF NOT EXISTS avatar_file (
    url TEXT PRIMARY KEY,
    file_id TEXT NOT NULL
);
"""


def _history_row(row) -> HistoryRow:  # строка sqlite
    return HistoryRow(
        row_id=int(row[0]),
        channel=str(row[1]),
        chat_id=int(row[2]),
        message_id=int(row[3]),
        label=str(row[4]),
        kind=str(row[5]),
        text=str(row[6] or ""),
        hub_channel=str(row[7]) if row[7] is not None else None,
        hub_chat_id=int(row[8]) if row[8] is not None else None,
        hub_message_id=int(row[9]) if row[9] is not None else None,
        ts=int(row[10] or 0),
        deleted=bool(row[11]),
    )


HISTORY_FIELDS = (
    "id, channel, chat_id, message_id, label, kind, text, "
    "hub_channel, hub_chat_id, hub_message_id, ts, deleted"
)


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def init(self) -> None:
        self._db = await aiosqlite.connect(self.path)
        await self._db.executescript(SCHEMA)
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Store не инициализирован")
        return self._db

    # --- владелец -------------------------------------------------------

    async def get_owner(self) -> int | None:
        async with self.db.execute("SELECT value FROM kv WHERE key = 'owner_id'") as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else None

    async def set_owner(self, user_id: int) -> None:
        await self.db.execute(
            "INSERT INTO kv(key, value) VALUES('owner_id', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(user_id),),
        )
        await self.db.commit()

    # --- настройки ------------------------------------------------------

    async def all_settings(self) -> dict[str, str]:
        async with self.db.execute("SELECT key, value FROM settings") as cur:
            rows = await cur.fetchall()
        return {str(row[0]): str(row[1]) for row in rows}

    async def set_setting(self, key: str, value: str) -> None:
        await self.db.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await self.db.commit()

    # --- активный чат ---------------------------------------------------

    async def set_active(
        self, user_id: int, channel: str, chat_id: int, title: str
    ) -> None:
        await self.db.execute(
            "INSERT INTO active_chat(user_id, channel, chat_id, title) VALUES(?, ?, ?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET channel = excluded.channel, "
            "chat_id = excluded.chat_id, title = excluded.title, "
            "updated = strftime('%s', 'now')",
            (user_id, channel, chat_id, title),
        )
        await self.db.commit()

    async def get_active(self, user_id: int) -> tuple[str, int, str] | None:
        async with self.db.execute(
            "SELECT channel, chat_id, title FROM active_chat WHERE user_id = ?",
            (user_id,),
        ) as cur:
            row = await cur.fetchone()
        return (str(row[0]), int(row[1]), str(row[2])) if row else None

    async def clear_active(self, user_id: int) -> None:
        await self.db.execute("DELETE FROM active_chat WHERE user_id = ?", (user_id,))
        await self.db.commit()

    # --- маршруты -------------------------------------------------------

    async def remember_route(
        self,
        hub_channel: str,
        hub_chat_id: int,
        hub_message_id: int,
        channel: str,
        chat_id: int,
        message_id: int | None,
        title: str,
        ts: int | None = None,
    ) -> None:
        """Запоминает, куда отвечать на сообщение, показанное в главном канале."""
        await self.db.execute(
            "INSERT OR REPLACE INTO routes"
            "(hub_channel, hub_chat_id, hub_message_id, channel, chat_id, message_id, "
            " title, ts, read) VALUES(?, ?, ?, ?, ?, ?, ?, ?, "
            " COALESCE((SELECT read FROM routes WHERE hub_channel = ? AND "
            "  hub_chat_id = ? AND hub_message_id = ?), 0))",
            (
                hub_channel,
                hub_chat_id,
                hub_message_id,
                channel,
                chat_id,
                message_id,
                title,
                ts,
                hub_channel,
                hub_chat_id,
                hub_message_id,
            ),
        )
        await self.db.commit()

    async def route_for(
        self, hub_channel: str, hub_chat_id: int, hub_message_id: int
    ) -> RouteRow | None:
        async with self.db.execute(
            "SELECT hub_channel, hub_chat_id, hub_message_id, channel, chat_id, "
            "message_id, title, ts FROM routes "
            "WHERE hub_channel = ? AND hub_chat_id = ? AND hub_message_id = ?",
            (hub_channel, hub_chat_id, hub_message_id),
        ) as cur:
            row = await cur.fetchone()
        if not row:
            return None
        return RouteRow(
            hub_channel=str(row[0]),
            hub_chat_id=int(row[1]),
            hub_message_id=int(row[2]),
            channel=str(row[3]),
            chat_id=int(row[4]),
            message_id=int(row[5]) if row[5] is not None else None,
            title=str(row[6]),
            ts=int(row[7] or 0),
        )

    async def routes_to_source(
        self, channel: str, chat_id: int, message_id: int
    ) -> list[RouteRow]:
        """Где это сообщение показано в главном канале."""
        async with self.db.execute(
            "SELECT hub_channel, hub_chat_id, hub_message_id, channel, chat_id, "
            "message_id, title, ts FROM routes "
            "WHERE channel = ? AND chat_id = ? AND message_id = ? "
            "ORDER BY hub_message_id",
            (channel, chat_id, message_id),
        ) as cur:
            rows = await cur.fetchall()
        return [
            RouteRow(
                hub_channel=str(row[0]),
                hub_chat_id=int(row[1]),
                hub_message_id=int(row[2]),
                channel=str(row[3]),
                chat_id=int(row[4]),
                message_id=int(row[5]) if row[5] is not None else None,
                title=str(row[6]),
                ts=int(row[7] or 0),
            )
            for row in rows
        ]

    async def routes_into_hub(
        self, hub_channel: str, hub_chat_id: int
    ) -> list[RouteRow]:
        """Все маршруты, показанные в этом чате главного канала."""
        async with self.db.execute(
            "SELECT hub_channel, hub_chat_id, hub_message_id, channel, chat_id, "
            "message_id, title, ts FROM routes "
            "WHERE hub_channel = ? AND hub_chat_id = ? ORDER BY hub_message_id",
            (hub_channel, hub_chat_id),
        ) as cur:
            rows = await cur.fetchall()
        return [
            RouteRow(
                hub_channel=str(row[0]),
                hub_chat_id=int(row[1]),
                hub_message_id=int(row[2]),
                channel=str(row[3]),
                chat_id=int(row[4]),
                message_id=int(row[5]) if row[5] is not None else None,
                title=str(row[6]),
                ts=int(row[7] or 0),
            )
            for row in rows
        ]

    async def unread_outgoing(
        self, channel: str, chat_id: int, mark: int
    ) -> list[RouteRow]:
        """Наши сообщения в чате, прочитанные до отметки mark."""
        async with self.db.execute(
            "SELECT hub_channel, hub_chat_id, hub_message_id, channel, chat_id, "
            "message_id, title, ts FROM routes "
            "WHERE channel = ? AND chat_id = ? AND read = 0 "
            "AND message_id IS NOT NULL AND ts IS NOT NULL AND ts <= ? ORDER BY ts",
            (channel, chat_id, mark),
        ) as cur:
            rows = await cur.fetchall()
        return [
            RouteRow(
                hub_channel=str(row[0]),
                hub_chat_id=int(row[1]),
                hub_message_id=int(row[2]),
                channel=str(row[3]),
                chat_id=int(row[4]),
                message_id=int(row[5]) if row[5] is not None else None,
                title=str(row[6]),
                ts=int(row[7] or 0),
            )
            for row in rows
        ]

    async def pending_outgoing(self, channel: str, chat_id: int) -> list[tuple[int, int]]:
        async with self.db.execute(
            "SELECT message_id, ts FROM routes "
            "WHERE channel = ? AND chat_id = ? AND read = 0 AND message_id IS NOT NULL "
            "ORDER BY ts",
            (channel, chat_id),
        ) as cur:
            rows = await cur.fetchall()
        return [(int(row[0]), int(row[1] or 0)) for row in rows]

    async def mark_read(
        self, channel: str, chat_id: int, message_ids: list[int]
    ) -> None:
        if not message_ids:
            return
        marks = ",".join("?" * len(message_ids))
        await self.db.execute(
            f"UPDATE routes SET read = 1 WHERE channel = ? AND chat_id = ? "
            f"AND message_id IN ({marks})",
            (channel, chat_id, *message_ids),
        )
        await self.db.commit()

    # --- игнорируемые чаты ----------------------------------------------

    async def muted_chats(self) -> list[tuple[str, int, str]]:
        async with self.db.execute(
            "SELECT channel, chat_id, title FROM muted WHERE muted = 1 "
            "ORDER BY title COLLATE NOCASE"
        ) as cur:
            rows = await cur.fetchall()
        return [(str(row[0]), int(row[1]), str(row[2])) for row in rows]

    async def muted_chats_count(self) -> int:
        """Считает кол-во игнорируеммых чатов через SELECT COUNT(*)"""

        async with self.db.execute(
            "SELECT COUNT(*) FROM muted WHERE muted = 1"
        ) as cur:
            result = await cur.fetchone()
            result = result[0] if result is not None else 0

            return int(result)

        return 0 # TODO: fix

    async def is_muted(self, channel: str, chat_id: int) -> bool:
        async with self.db.execute(
            "SELECT muted FROM muted WHERE channel = ? AND chat_id = ?",
            (channel, chat_id),
        ) as cur:
            row = await cur.fetchone()
        return bool(row and row[0])

    async def set_muted(
        self, channel: str, chat_id: int, title: str, muted: bool
    ) -> None:
        if muted:
            await self.db.execute(
                "INSERT INTO muted(channel, chat_id, title, muted) VALUES(?, ?, ?, 1) "
                "ON CONFLICT(channel, chat_id) DO UPDATE SET "
                "title = excluded.title, muted = 1",
                (channel, chat_id, title),
            )
        else:
            await self.db.execute(
                "DELETE FROM muted WHERE channel = ? AND chat_id = ?",
                (channel, chat_id),
            )
        await self.db.commit()

    # --- история сообщений ----------------------------------------------

    async def add_history(
        self,
        channel: str,
        chat_id: int,
        message_id: int,
        sender: int | None,
        label: str,
        ts: int,
        kind: str = "text",
        text: str | None = None,
    ) -> None:
        """Добавляет сообщение в историю; правка создаёт новую версию."""
        await self.db.execute(
            "INSERT INTO messages"
            "(channel, chat_id, message_id, version, sender, label, ts, kind, text) "
            "VALUES(?, ?, ?, COALESCE((SELECT MAX(version) + 1 FROM messages "
            "WHERE channel = ? AND chat_id = ? AND message_id = ?), 0), ?, ?, ?, ?, ?) "
            "ON CONFLICT(channel, chat_id, message_id, version) DO UPDATE SET "
            "label = excluded.label, kind = excluded.kind, text = excluded.text, "
            "ts = excluded.ts, deleted = 0",
            (
                channel,
                chat_id,
                message_id,
                channel,
                chat_id,
                message_id,
                sender,
                label,
                ts,
                kind,
                text,
            ),
        )
        await self.db.commit()

    async def has_history(self, channel: str, chat_id: int, message_id: int) -> bool:
        async with self.db.execute(
            "SELECT 1 FROM messages WHERE channel = ? AND chat_id = ? AND message_id = ? "
            "LIMIT 1",
            (channel, chat_id, message_id),
        ) as cur:
            return await cur.fetchone() is not None

    async def record_own_message(
        self,
        channel: str,
        chat_id: int,
        message_id: int,
        hub_message_id: int | None,
        text: str | None,
        kind: str,
        ts: int,
        hub_channel: str | None = None,
        hub_chat_id: int | None = None,
    ) -> None:
        """Своё сообщение: ушло из главного канала или отправлено с другого устройства."""
        if await self.has_history(channel, chat_id, message_id):
            await self.set_history_hub(
                channel, chat_id, message_id, hub_channel, hub_chat_id, hub_message_id
            )
            return
        await self.add_history(channel, chat_id, message_id, None, "Я", ts, kind, text)
        await self.set_history_hub(
            channel, chat_id, message_id, hub_channel, hub_chat_id, hub_message_id
        )

    async def set_history_hub(
        self,
        channel: str,
        chat_id: int,
        message_id: int,
        hub_channel: str | None,
        hub_chat_id: int | None,
        hub_message_id: int | None,
        version: int | None = None,
    ) -> None:
        """Привязывает запись истории к сообщению в главном канале."""
        if hub_message_id is None:
            return
        if version is None:
            await self.db.execute(
                "UPDATE messages SET hub_channel = ?, hub_chat_id = ?, hub_message_id = ?, "
                "forwarded = 1 WHERE channel = ? AND chat_id = ? AND message_id = ? "
                "AND version = (SELECT MAX(version) FROM messages "
                "WHERE channel = ? AND chat_id = ? AND message_id = ?)",
                (
                    hub_channel,
                    hub_chat_id,
                    hub_message_id,
                    channel,
                    chat_id,
                    message_id,
                    channel,
                    chat_id,
                    message_id,
                ),
            )
        else:
            await self.db.execute(
                "UPDATE messages SET hub_channel = ?, hub_chat_id = ?, hub_message_id = ?, "
                "forwarded = 1 WHERE channel = ? AND chat_id = ? AND message_id = ? "
                "AND version = ?",
                (
                    hub_channel,
                    hub_chat_id,
                    hub_message_id,
                    channel,
                    chat_id,
                    message_id,
                    version,
                ),
            )
        await self.db.commit()

    async def set_history_text(
        self, channel: str, chat_id: int, message_id: int, text: str | None
    ) -> None:
        await self.db.execute(
            "UPDATE messages SET text = ? WHERE channel = ? AND chat_id = ? "
            "AND message_id = ? AND version = (SELECT MAX(version) FROM messages "
            "WHERE channel = ? AND chat_id = ? AND message_id = ?)",
            (text, channel, chat_id, message_id, channel, chat_id, message_id),
        )
        await self.db.commit()

    async def mark_history_deleted(
        self,
        channel: str,
        chat_id: int,
        message_id: int,
        only_latest: bool = False,
    ) -> bool:
        if only_latest:
            query = (
                "UPDATE messages SET deleted = 1, hub_message_id = NULL "
                "WHERE channel = ? AND chat_id = ? AND message_id = ? AND version = "
                "(SELECT MAX(version) FROM messages "
                "WHERE channel = ? AND chat_id = ? AND message_id = ?)"
            )
            params: tuple = (channel, chat_id, message_id, channel, chat_id, message_id)
        else:
            query = (
                "UPDATE messages SET deleted = 1, hub_message_id = NULL "
                "WHERE channel = ? AND chat_id = ? AND message_id = ?"
            )
            params = (channel, chat_id, message_id)
        async with self.db.execute(query, params) as cur:
            changed = cur.rowcount
        await self.db.commit()
        return bool(changed)

    async def history_newest(
        self, channel: str, chat_id: int, limit: int = 8
    ) -> list[HistoryRow]:
        """Самые свежие записи истории, от новых к старым."""
        return await self.history_before(channel, chat_id, None, limit)

    async def history_before(
        self, channel: str, chat_id: int, before: int | None, limit: int = 8
    ) -> list[HistoryRow]:
        """Записи старше сообщения ``before`` (``None`` — с самых свежих).

        Страница привязана к id сообщения в мессенджере, а не к смещению,
        поэтому догрузка, попадающая в середину списка, не сдвигает окно.
        """
        query = f"SELECT {HISTORY_FIELDS} FROM messages WHERE channel = ? AND chat_id = ?"
        params: list = [channel, chat_id]
        if before is not None:
            query += " AND message_id < ?"
            params.append(before)
        query += " ORDER BY message_id DESC, id DESC LIMIT ?"
        params.append(limit)
        async with self.db.execute(query, tuple(params)) as cur:
            rows = await cur.fetchall()
        return [_history_row(row) for row in rows]

    async def history_after(
        self, channel: str, chat_id: int, after: int, limit: int = 8
    ) -> list[HistoryRow]:
        """Записи новее сообщения ``after`` — ближайшие к нему, от новых к старым."""
        async with self.db.execute(
            f"SELECT {HISTORY_FIELDS} FROM messages "
            "WHERE channel = ? AND chat_id = ? AND message_id > ? "
            "ORDER BY message_id ASC, id ASC LIMIT ?",
            (channel, chat_id, after, limit),
        ) as cur:
            rows = await cur.fetchall()
        return [_history_row(row) for row in reversed(rows)]

    async def history_newer_count(self, channel: str, chat_id: int, message_id: int) -> int:
        """Сколько записей в истории новее указанной (для номера страницы)."""
        async with self.db.execute(
            "SELECT COUNT(*) FROM messages "
            "WHERE channel = ? AND chat_id = ? AND message_id > ?",
            (channel, chat_id, message_id),
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def history_count(self, channel: str, chat_id: int) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) FROM messages WHERE channel = ? AND chat_id = ?",
            (channel, chat_id),
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else 0

    async def history_row(self, row_id: int) -> HistoryRow | None:
        async with self.db.execute(
            f"SELECT {HISTORY_FIELDS} FROM messages WHERE id = ?", (row_id,)
        ) as cur:
            row = await cur.fetchone()
        return _history_row(row) if row else None

    async def last_message_id(self, channel: str, chat_id: int) -> int | None:
        async with self.db.execute(
            "SELECT message_id FROM messages WHERE channel = ? AND chat_id = ? "
            "ORDER BY message_id DESC LIMIT 1",
            (channel, chat_id),
        ) as cur:
            row = await cur.fetchone()
        return int(row[0]) if row else None

    # --- аватарки (Telegram file_id) ------------------------------------

    async def get_avatar_file(self, url: str) -> str | None:
        async with self.db.execute(
            "SELECT file_id FROM avatar_file WHERE url = ?", (url,)
        ) as cur:
            row = await cur.fetchone()
        return str(row[0]) if row else None

    async def set_avatar_file(self, url: str, file_id: str) -> None:
        await self.db.execute(
            "INSERT INTO avatar_file(url, file_id) VALUES(?, ?) "
            "ON CONFLICT(url) DO UPDATE SET file_id = excluded.file_id",
            (url, file_id),
        )
        await self.db.commit()
