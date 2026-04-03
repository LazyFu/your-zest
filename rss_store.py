import json
import sqlite3
import threading
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "utm_id",
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "spm",
}


def canonicalize_link(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""

    try:
        parsed = urlsplit(raw)
    except ValueError:
        return raw

    cleaned_query = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in TRACKING_QUERY_KEYS:
            continue
        cleaned_query.append((key, value))

    query = urlencode(cleaned_query, doseq=True)
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    return urlunsplit((parsed.scheme.lower(), netloc, path, query, ""))


def build_content_hash(title: str, summary: str, content: str) -> str:
    base = " ".join(
        part.strip().lower()
        for part in [title or "", summary or "", (content or "")[:1000]]
        if part and part.strip()
    )
    if not base:
        return ""
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


@dataclass
class Feed:
    id: int
    url: str
    title: str | None
    db_id: int | None = None


@dataclass
class FeedItem:
    id: int
    feed_id: int
    title: str
    link: str
    summary: str
    published_at: str | None


@dataclass
class FeedItemDetail:
    id: int
    feed_id: int
    title: str
    link: str
    summary: str
    content: str
    published_at: str | None


class SqliteStore:
    def __init__(self, db_path: str = "rss_data.db") -> None:
        self.db_path = db_path
        Path(db_path).touch(exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS feeds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_no INTEGER NOT NULL UNIQUE,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT,
                    created_at TEXT NOT NULL,
                    last_checked_at TEXT,
                    last_error TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS user_subscriptions (
                    chat_id INTEGER NOT NULL,
                    feed_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, feed_id),
                    FOREIGN KEY(feed_id) REFERENCES feeds(id)
                );

                CREATE TABLE IF NOT EXISTS feed_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    feed_id INTEGER NOT NULL,
                    guid TEXT NOT NULL,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    canonical_link TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    content TEXT NOT NULL DEFAULT '',
                    content_hash TEXT NOT NULL DEFAULT '',
                    published_at TEXT,
                    inserted_at TEXT NOT NULL,
                    duplicate_of_item_id INTEGER,
                    FOREIGN KEY(feed_id) REFERENCES feeds(id),
                    FOREIGN KEY(duplicate_of_item_id) REFERENCES feed_items(id),
                    UNIQUE(feed_id, guid)
                );

                CREATE TABLE IF NOT EXISTS user_preferences (
                    chat_id INTEGER PRIMARY KEY,
                    keywords_json TEXT NOT NULL,
                    rating_scale INTEGER NOT NULL DEFAULT 5,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_source_preferences (
                    chat_id INTEGER NOT NULL,
                    feed_id INTEGER NOT NULL,
                    weight REAL NOT NULL,
                    positive_count INTEGER NOT NULL DEFAULT 0,
                    negative_count INTEGER NOT NULL DEFAULT 0,
                    last_feedback_at TEXT NOT NULL,
                    PRIMARY KEY(chat_id, feed_id),
                    FOREIGN KEY(feed_id) REFERENCES feeds(id)
                );

                CREATE TABLE IF NOT EXISTS user_feedback (
                    chat_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    feedback TEXT NOT NULL,
                    feedback_value INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, item_id),
                    FOREIGN KEY(item_id) REFERENCES feed_items(id)
                );

                CREATE TABLE IF NOT EXISTS user_interest_terms (
                    chat_id INTEGER NOT NULL,
                    term TEXT NOT NULL,
                    weight REAL NOT NULL,
                    positive_count INTEGER NOT NULL DEFAULT 0,
                    negative_count INTEGER NOT NULL DEFAULT 0,
                    last_feedback_at TEXT NOT NULL,
                    PRIMARY KEY (chat_id, term)
                );

                CREATE TABLE IF NOT EXISTS sent_items (
                    chat_id INTEGER NOT NULL,
                    item_id INTEGER NOT NULL,
                    sent_at TEXT NOT NULL,
                    PRIMARY KEY(chat_id, item_id),
                    FOREIGN KEY(item_id) REFERENCES feed_items(id)
                );

                CREATE INDEX IF NOT EXISTS idx_feed_items_published_at
                ON feed_items(COALESCE(published_at, inserted_at) DESC);

                CREATE INDEX IF NOT EXISTS idx_feed_items_canonical_link
                ON feed_items(canonical_link);

                CREATE INDEX IF NOT EXISTS idx_feed_items_content_hash
                ON feed_items(content_hash);

                CREATE INDEX IF NOT EXISTS idx_feed_items_duplicate
                ON feed_items(duplicate_of_item_id);

                CREATE INDEX IF NOT EXISTS idx_user_interest_terms_weight
                ON user_interest_terms(chat_id, weight DESC);

                CREATE INDEX IF NOT EXISTS idx_user_source_preferences_weight
                ON user_source_preferences(chat_id, weight DESC);
                """
            )
            self._run_migrations(conn)

    def _run_migrations(self, conn: sqlite3.Connection) -> None:
        feed_columns = {
            str(row["name"]).lower()
            for row in conn.execute("PRAGMA table_info(feeds)").fetchall()
        }
        if "display_no" not in feed_columns:
            conn.execute("ALTER TABLE feeds ADD COLUMN display_no INTEGER")
            existing_rows = conn.execute("SELECT id FROM feeds ORDER BY id").fetchall()
            for index, row in enumerate(existing_rows, start=1):
                conn.execute(
                    "UPDATE feeds SET display_no = ? WHERE id = ?",
                    (index, int(row["id"])),
                )

        item_columns = {
            str(row["name"]).lower()
            for row in conn.execute("PRAGMA table_info(feed_items)").fetchall()
        }
        if "canonical_link" not in item_columns:
            conn.execute("ALTER TABLE feed_items ADD COLUMN canonical_link TEXT NOT NULL DEFAULT ''")
        if "content_hash" not in item_columns:
            conn.execute("ALTER TABLE feed_items ADD COLUMN content_hash TEXT NOT NULL DEFAULT ''")
        if "duplicate_of_item_id" not in item_columns:
            conn.execute("ALTER TABLE feed_items ADD COLUMN duplicate_of_item_id INTEGER")

        user_pref_columns = {
            str(row["name"]).lower()
            for row in conn.execute("PRAGMA table_info(user_preferences)").fetchall()
        }
        if "rating_scale" not in user_pref_columns:
            conn.execute("ALTER TABLE user_preferences ADD COLUMN rating_scale INTEGER NOT NULL DEFAULT 5")

        conn.executescript(
            """
            UPDATE feed_items
            SET canonical_link = link
            WHERE canonical_link = '';

            CREATE TABLE IF NOT EXISTS user_feedback (
                chat_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                feedback TEXT NOT NULL,
                feedback_value INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, item_id),
                FOREIGN KEY(item_id) REFERENCES feed_items(id)
            );

            CREATE TABLE IF NOT EXISTS user_interest_terms (
                chat_id INTEGER NOT NULL,
                term TEXT NOT NULL,
                weight REAL NOT NULL,
                positive_count INTEGER NOT NULL DEFAULT 0,
                negative_count INTEGER NOT NULL DEFAULT 0,
                last_feedback_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, term)
            );

            CREATE TABLE IF NOT EXISTS user_source_preferences (
                chat_id INTEGER NOT NULL,
                feed_id INTEGER NOT NULL,
                weight REAL NOT NULL,
                positive_count INTEGER NOT NULL DEFAULT 0,
                negative_count INTEGER NOT NULL DEFAULT 0,
                last_feedback_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, feed_id),
                FOREIGN KEY(feed_id) REFERENCES feeds(id)
            );

            CREATE INDEX IF NOT EXISTS idx_feed_items_published_at
            ON feed_items(COALESCE(published_at, inserted_at) DESC);

            CREATE INDEX IF NOT EXISTS idx_feed_items_canonical_link
            ON feed_items(canonical_link);

            CREATE INDEX IF NOT EXISTS idx_feed_items_content_hash
            ON feed_items(content_hash);

            CREATE INDEX IF NOT EXISTS idx_feed_items_duplicate
            ON feed_items(duplicate_of_item_id);

            CREATE INDEX IF NOT EXISTS idx_user_interest_terms_weight
            ON user_interest_terms(chat_id, weight DESC);

            CREATE INDEX IF NOT EXISTS idx_user_source_preferences_weight
            ON user_source_preferences(chat_id, weight DESC);
            """
        )

    def _next_display_no(self, conn: sqlite3.Connection) -> int:
        rows = conn.execute("SELECT display_no FROM feeds ORDER BY display_no").fetchall()
        expected = 1
        for row in rows:
            current = int(row["display_no"])
            if current > expected:
                break
            expected = current + 1
        return expected

    def _resolve_feed_db_id(self, conn: sqlite3.Connection, feed_no: int) -> int | None:
        row = conn.execute(
            "SELECT id FROM feeds WHERE display_no = ?",
            (feed_no,),
        ).fetchone()
        return int(row["id"]) if row else None

    def _sync_sqlite_sequence(self, conn: sqlite3.Connection, table_name: str) -> None:
        max_row = conn.execute(f"SELECT COALESCE(MAX(id), 0) AS max_id FROM {table_name}").fetchone()
        max_id = int(max_row["max_id"]) if max_row else 0
        existing = conn.execute(
            "SELECT 1 FROM sqlite_sequence WHERE name = ?",
            (table_name,),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE sqlite_sequence SET seq = ? WHERE name = ?",
                (max_id, table_name),
            )
        else:
            conn.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)",
                (table_name, max_id),
            )

    def upsert_feed(self, url: str, title: str | None = None) -> Feed:
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT id, display_no, url, title FROM feeds WHERE url = ?", (url.strip(),)).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE feeds
                    SET title = COALESCE(?, title),
                        is_active = 1
                    WHERE id = ?
                    """,
                    (title, int(row["id"])),
                )
                row = conn.execute(
                    "SELECT id, display_no, url, title FROM feeds WHERE id = ?",
                    (int(row["id"]),),
                ).fetchone()
                return Feed(id=int(row["display_no"]), url=str(row["url"]), title=row["title"], db_id=int(row["id"]))

            display_no = self._next_display_no(conn)
            conn.execute(
                """
                INSERT INTO feeds(display_no, url, title, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title = COALESCE(excluded.title, feeds.title),
                    is_active = 1
                """,
                (display_no, url.strip(), title, now_iso()),
            )
            row = conn.execute("SELECT id, display_no, url, title FROM feeds WHERE url = ?", (url.strip(),)).fetchone()
            return Feed(id=int(row["display_no"]), url=str(row["url"]), title=row["title"], db_id=int(row["id"]))

    def subscribe(self, chat_id: int, feed_id: int) -> None:
        with self._lock, self._connect() as conn:
            db_id = self._resolve_feed_db_id(conn, feed_id)
            if db_id is None:
                raise ValueError(f"feed not found: {feed_id}")
            conn.execute(
                """
                INSERT OR IGNORE INTO user_subscriptions(chat_id, feed_id, created_at)
                VALUES (?, ?, ?)
                """,
                (chat_id, db_id, now_iso()),
            )

    def unsubscribe(self, chat_id: int, feed_id: int) -> bool:
        with self._lock, self._connect() as conn:
            db_id = self._resolve_feed_db_id(conn, feed_id)
            if db_id is None:
                return False
            cur = conn.execute(
                "DELETE FROM user_subscriptions WHERE chat_id = ? AND feed_id = ?",
                (chat_id, db_id),
            )
            return cur.rowcount > 0

    def delete_feed(self, feed_id: int) -> bool:
        with self._lock, self._connect() as conn:
            db_id = self._resolve_feed_db_id(conn, feed_id)
            if db_id is None:
                return False

            item_ids = [int(row["id"]) for row in conn.execute("SELECT id FROM feed_items WHERE feed_id = ?", (db_id,)).fetchall()]
            if item_ids:
                placeholders = ",".join("?" for _ in item_ids)
                conn.execute(
                    f"DELETE FROM sent_items WHERE item_id IN ({placeholders})",
                    item_ids,
                )
                conn.execute(
                    f"DELETE FROM user_feedback WHERE item_id IN ({placeholders})",
                    item_ids,
                )
                conn.execute(
                    f"UPDATE feed_items SET duplicate_of_item_id = NULL WHERE duplicate_of_item_id IN ({placeholders})",
                    item_ids,
                )
                conn.execute("DELETE FROM feed_items WHERE feed_id = ?", (db_id,))

            conn.execute("DELETE FROM user_subscriptions WHERE feed_id = ?", (db_id,))
            conn.execute("DELETE FROM user_source_preferences WHERE feed_id = ?", (db_id,))
            conn.execute("DELETE FROM feeds WHERE id = ?", (db_id,))
            self._sync_sqlite_sequence(conn, "feed_items")
            self._sync_sqlite_sequence(conn, "feeds")
            return True

    def get_feed_by_display_no(self, feed_id: int) -> Feed | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, display_no, url, title FROM feeds WHERE display_no = ?",
                (feed_id,),
            ).fetchone()
            if not row:
                return None
            return Feed(id=int(row["display_no"]), url=str(row["url"]), title=row["title"], db_id=int(row["id"]))

    def list_subscriptions(self, chat_id: int) -> list[Feed]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT f.id, f.display_no, f.url, f.title
                FROM user_subscriptions s
                JOIN feeds f ON s.feed_id = f.id
                WHERE s.chat_id = ? AND f.is_active = 1
                ORDER BY f.display_no ASC
                """,
                (chat_id,),
            ).fetchall()
            return [Feed(id=int(r["display_no"]), url=str(r["url"]), title=r["title"], db_id=int(r["id"])) for r in rows]

    def list_all_active_feeds(self) -> list[Feed]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, display_no, url, title FROM feeds WHERE is_active = 1 ORDER BY display_no"
            ).fetchall()
            return [Feed(id=int(r["display_no"]), url=str(r["url"]), title=r["title"], db_id=int(r["id"])) for r in rows]

    def update_feed_status(self, feed_id: int, error: str | None) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE feeds
                SET last_checked_at = ?, last_error = ?
                WHERE id = ?
                """,
                (now_iso(), error, feed_id),
            )

    def insert_items(self, feed_id: int, entries: Iterable[dict]) -> list[int]:
        new_ids: list[int] = []
        with self._lock, self._connect() as conn:
            for entry in entries:
                title = str(entry.get("title", ""))
                link = str(entry.get("link", ""))
                summary = str(entry.get("summary", ""))
                content = str(entry.get("content", ""))
                canonical_link = canonicalize_link(link)
                content_hash = build_content_hash(title, summary, content)

                duplicate_of_item_id = None
                if canonical_link:
                    dup = conn.execute(
                        """
                        SELECT id
                        FROM feed_items
                        WHERE canonical_link = ?
                        ORDER BY COALESCE(published_at, inserted_at) DESC
                        LIMIT 1
                        """,
                        (canonical_link,),
                    ).fetchone()
                    if dup:
                        duplicate_of_item_id = int(dup["id"])

                if duplicate_of_item_id is None and content_hash:
                    dup = conn.execute(
                        """
                        SELECT id
                        FROM feed_items
                        WHERE content_hash = ?
                        ORDER BY COALESCE(published_at, inserted_at) DESC
                        LIMIT 1
                        """,
                        (content_hash,),
                    ).fetchone()
                    if dup:
                        duplicate_of_item_id = int(dup["id"])

                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO feed_items(
                        feed_id, guid, title, link, canonical_link, summary, content,
                        content_hash, published_at, inserted_at, duplicate_of_item_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        feed_id,
                        entry["guid"],
                        title,
                        link,
                        canonical_link,
                        summary,
                        content,
                        content_hash,
                        entry.get("published_at"),
                        now_iso(),
                        duplicate_of_item_id,
                    ),
                )
                if cur.rowcount > 0:
                    new_row = conn.execute(
                        "SELECT id FROM feed_items WHERE feed_id = ? AND guid = ?",
                        (feed_id, entry["guid"]),
                    ).fetchone()
                    if new_row:
                        new_ids.append(new_row["id"])
        return new_ids

    def get_candidate_items_for_chat(self, chat_id: int, limit: int = 50) -> list[FeedItem]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT i.id, i.feed_id, i.title, i.link, i.summary, i.published_at
                FROM user_subscriptions s
                JOIN feed_items i ON i.feed_id = s.feed_id
                LEFT JOIN sent_items si ON si.item_id = i.id AND si.chat_id = s.chat_id
                WHERE s.chat_id = ?
                  AND si.item_id IS NULL
                  AND i.duplicate_of_item_id IS NULL
                ORDER BY COALESCE(i.published_at, i.inserted_at) DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
            return [
                FeedItem(
                    id=r["id"],
                    feed_id=r["feed_id"],
                    title=r["title"],
                    link=r["link"],
                    summary=r["summary"],
                    published_at=r["published_at"],
                )
                for r in rows
            ]

    def list_chat_ids(self) -> list[int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT chat_id FROM user_subscriptions").fetchall()
            return [int(r["chat_id"]) for r in rows]

    def mark_sent(self, chat_id: int, item_id: int) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO sent_items(chat_id, item_id, sent_at)
                VALUES (?, ?, ?)
                """,
                (chat_id, item_id, now_iso()),
            )

    def set_keywords(self, chat_id: int, keywords: list[str]) -> None:
        incoming = {k.strip().lower() for k in keywords if k.strip()}
        with self._lock, self._connect() as conn:
            existing_row = conn.execute(
                "SELECT keywords_json FROM user_preferences WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()

            existing: set[str] = set()
            if existing_row and existing_row["keywords_json"]:
                try:
                    values = json.loads(existing_row["keywords_json"])
                    if isinstance(values, list):
                        existing = {str(v).strip().lower() for v in values if str(v).strip()}
                except json.JSONDecodeError:
                    existing = set()

            cleaned = sorted(existing | incoming)
            conn.execute(
                """
                INSERT INTO user_preferences(chat_id, keywords_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    keywords_json = excluded.keywords_json,
                    updated_at = excluded.updated_at
                """,
                (chat_id, json.dumps(cleaned, ensure_ascii=True), now_iso()),
            )

            # Manual keywords are strong long-term preference signals.
            for term in cleaned:
                conn.execute(
                    """
                    INSERT INTO user_interest_terms(
                        chat_id, term, weight, positive_count, negative_count, last_feedback_at
                    ) VALUES (?, ?, ?, 1, 0, ?)
                    ON CONFLICT(chat_id, term) DO UPDATE SET
                        weight = MAX(user_interest_terms.weight, excluded.weight),
                        positive_count = user_interest_terms.positive_count + 1,
                        last_feedback_at = excluded.last_feedback_at
                    """,
                    (chat_id, term, 3.0, now_iso()),
                )

    def remove_keywords(self, chat_id: int, keywords: list[str]) -> list[str]:
        to_remove = {k.strip().lower() for k in keywords if k.strip()}
        if not to_remove:
            return self.get_keywords(chat_id)

        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT keywords_json FROM user_preferences WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
            if not row:
                return []

            try:
                values = json.loads(row["keywords_json"])
                if isinstance(values, list):
                    current = {str(v).strip().lower() for v in values if str(v).strip()}
                else:
                    current = set()
            except json.JSONDecodeError:
                current = set()

            updated = sorted(current - to_remove)
            conn.execute(
                """
                UPDATE user_preferences
                SET keywords_json = ?, updated_at = ?
                WHERE chat_id = ?
                """,
                (json.dumps(updated, ensure_ascii=True), now_iso(), chat_id),
            )
            return updated

    def set_rating_scale(self, chat_id: int, scale: int) -> None:
        normalized = 3 if int(scale) == 3 else 5
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT keywords_json FROM user_preferences WHERE chat_id = ?", (chat_id,)).fetchone()
            keywords_json = row["keywords_json"] if row else "[]"
            conn.execute(
                """
                INSERT INTO user_preferences(chat_id, keywords_json, rating_scale, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    rating_scale = excluded.rating_scale,
                    updated_at = excluded.updated_at
                """,
                (chat_id, keywords_json, normalized, now_iso()),
            )

    def get_rating_scale(self, chat_id: int) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT rating_scale FROM user_preferences WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
            if not row:
                return 5
            scale = int(row["rating_scale"])
            return 3 if scale == 3 else 5

    def get_keywords(self, chat_id: int) -> list[str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT keywords_json FROM user_preferences WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
            if not row:
                return []
            try:
                keywords = json.loads(row["keywords_json"])
                if isinstance(keywords, list):
                    return [str(k) for k in keywords]
            except json.JSONDecodeError:
                return []
            return []

    def record_feedback(self, chat_id: int, item_id: int, feedback: str, feedback_value: int) -> None:
        feedback_type = feedback.strip().lower()
        if not feedback_type:
            raise ValueError("feedback cannot be empty")

        with self._lock, self._connect() as conn:
            now = now_iso()
            existing = conn.execute(
                "SELECT feedback_value FROM user_feedback WHERE chat_id = ? AND item_id = ?",
                (chat_id, item_id),
            ).fetchone()
            old_value = int(existing["feedback_value"]) if existing else 0
            delta_value = int(feedback_value) - old_value
            conn.execute(
                """
                INSERT INTO user_feedback(chat_id, item_id, feedback, feedback_value, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, item_id) DO UPDATE SET
                    feedback = excluded.feedback,
                    feedback_value = excluded.feedback_value,
                    updated_at = excluded.updated_at
                """,
                (chat_id, item_id, feedback_type, int(feedback_value), now, now),
            )

            if delta_value == 0:
                return

            row = conn.execute(
                """
                SELECT feed_id, title, summary
                FROM feed_items
                WHERE id = ?
                """,
                (item_id,),
            ).fetchone()
            if not row:
                return

            feed_id = int(row["feed_id"])
            conn.execute(
                """
                INSERT INTO user_source_preferences(
                    chat_id, feed_id, weight, positive_count, negative_count, last_feedback_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, feed_id) DO UPDATE SET
                    weight = user_source_preferences.weight + excluded.weight,
                    positive_count = user_source_preferences.positive_count + excluded.positive_count,
                    negative_count = user_source_preferences.negative_count + excluded.negative_count,
                    last_feedback_at = excluded.last_feedback_at
                """,
                (
                    chat_id,
                    feed_id,
                    float(delta_value),
                    1 if delta_value > 0 else 0,
                    1 if delta_value < 0 else 0,
                    now,
                ),
            )

            text = f"{row['title']} {row['summary']}".lower()
            terms = {term for term in re_split_terms(text) if len(term) >= 2}
            if not terms:
                return

            for term in terms:
                conn.execute(
                    """
                    INSERT INTO user_interest_terms(
                        chat_id, term, weight, positive_count, negative_count, last_feedback_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?
                    )
                    ON CONFLICT(chat_id, term) DO UPDATE SET
                        weight = user_interest_terms.weight + excluded.weight,
                        positive_count = user_interest_terms.positive_count + excluded.positive_count,
                        negative_count = user_interest_terms.negative_count + excluded.negative_count,
                        last_feedback_at = excluded.last_feedback_at
                    """,
                    (
                        chat_id,
                        term,
                        float(delta_value),
                        1 if delta_value > 0 else 0,
                        1 if delta_value < 0 else 0,
                        now,
                    ),
                )

    def get_interest_terms(self, chat_id: int, limit: int = 100) -> list[tuple[str, float]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT term, weight
                FROM user_interest_terms
                WHERE chat_id = ?
                ORDER BY weight DESC, last_feedback_at DESC
                LIMIT ?
                """,
                (chat_id, limit),
            ).fetchall()
            return [(str(r["term"]), float(r["weight"])) for r in rows]

    def get_source_preferences(self, chat_id: int) -> dict[int, float]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT feed_id, weight
                FROM user_source_preferences
                WHERE chat_id = ?
                """,
                (chat_id,),
            ).fetchall()
            return {int(r["feed_id"]): float(r["weight"]) for r in rows}

    def get_item_detail(self, item_id: int) -> FeedItemDetail | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, feed_id, title, link, summary, content, published_at
                FROM feed_items
                WHERE id = ?
                """,
                (item_id,),
            ).fetchone()
            if not row:
                return None
            return FeedItemDetail(
                id=int(row["id"]),
                feed_id=int(row["feed_id"]),
                title=str(row["title"]),
                link=str(row["link"]),
                summary=str(row["summary"]),
                content=str(row["content"]),
                published_at=row["published_at"],
            )


def re_split_terms(text: str) -> list[str]:
    cleaned = []
    word = []
    for ch in text:
        if ch.isalnum() or ch in {"_", "-"}:
            word.append(ch)
        else:
            if word:
                cleaned.append("".join(word))
                word = []
    if word:
        cleaned.append("".join(word))
    return cleaned
