import html
import logging
import math
import re
from datetime import datetime, time as dt_time, timezone
from typing import Any, cast

import feedparser
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from rss_store import FeedItem, SqliteStore, re_split_terms


logger = logging.getLogger(__name__)
MAX_TELEGRAM_MESSAGE_LEN = 3500
PUSH_WINDOW_START = dt_time(hour=9, minute=0)
PUSH_WINDOW_END = dt_time(hour=23, minute=0)


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _build_feedback_keyboard(item_id: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("1", callback_data=f"fb:{item_id}:5:1"),
            InlineKeyboardButton("2", callback_data=f"fb:{item_id}:5:2"),
            InlineKeyboardButton("3", callback_data=f"fb:{item_id}:5:3"),
            InlineKeyboardButton("4", callback_data=f"fb:{item_id}:5:4"),
            InlineKeyboardButton("5", callback_data=f"fb:{item_id}:5:5"),
        ]
    ]
    return InlineKeyboardMarkup(rows)


def _in_push_window(now: datetime) -> bool:
    current = now.time()
    return PUSH_WINDOW_START <= current <= PUSH_WINDOW_END


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def parse_entry(entry: Any) -> dict[str, str | None]:
    title = str(entry.get("title", "(no title)")).strip()
    link = str(entry.get("link", "")).strip()

    summary = ""
    if "summary" in entry:
        summary = str(entry.get("summary", ""))

    content = ""
    content_list = entry.get("content", [])
    if content_list and isinstance(content_list, list):
        first = content_list[0]
        if isinstance(first, dict):
            content = str(first.get("value", ""))

    guid = str(
        entry.get("id")
        or entry.get("guid")
        or link
        or f"{title}-{entry.get('published', '')}"
    )

    published_at = str(entry.get("published", "")).strip() or None

    return {
        "guid": guid,
        "title": title,
        "link": link,
        "summary": _strip_html(summary),
        "content": _strip_html(content),
        "published_at": published_at,
    }


def score_item(item: FeedItem, keywords: list[str]) -> int:
    """Return the number of keyword matches in the item's title and summary."""
    if not keywords:
        return 0
    haystack = f"{item.title} {item.summary}".lower()
    score = 0
    for keyword in keywords:
        if keyword and keyword.lower() in haystack:
            score += 1
    return score


def rank_item(
    item: FeedItem,
    keywords: list[str],
    interest_terms: dict[str, float],
    source_weights: dict[int, float],
) -> float:
    text = f"{item.title} {item.summary}".lower()
    tokens = set(re_split_terms(text))

    keyword_score = 1.4 * score_item(item, keywords)

    interest_score = 0.0
    for token in tokens:
        weight = interest_terms.get(token)
        if weight is not None:
            interest_score += max(-2.0, min(4.0, weight * 0.15))

    source_score = max(-1.5, min(2.5, source_weights.get(item.feed_id, 0.0) * 0.18))

    now = datetime.now(timezone.utc)
    published_dt = _parse_iso(item.published_at)
    hours = 72.0
    if published_dt is not None:
        delta_hours = max(0.0, (now - published_dt).total_seconds() / 3600.0)
        hours = delta_hours
    freshness_score = 2.2 * math.exp(-hours / 36.0)

    return keyword_score + interest_score + source_score + freshness_score


def format_item_summary(item: FeedItem, score: int | None = None) -> str:
    parts = [f"- {item.title}"]
    if item.published_at:
        parts.append(f"  时间: {item.published_at}")
    if item.summary:
        parts.append(f"  摘要: {item.summary[:180]}")
    parts.append(f"  链接: {item.link}")
    if score is not None:
        parts.append(f"  相关度: {score}")
    return "\n".join(parts)


def _chunk_message_lines(lines: list[str], max_len: int = MAX_TELEGRAM_MESSAGE_LEN) -> list[str]:
    chunks: list[str] = []
    current = ""
    for line in lines:
        candidate = line if not current else f"{current}\n\n{line}"
        if len(candidate) <= max_len:
            current = candidate
            continue

        if current:
            chunks.append(current)
        if len(line) <= max_len:
            current = line
        else:
            start = 0
            while start < len(line):
                end = start + max_len
                chunks.append(line[start:end])
                start = end
            current = ""

    if current:
        chunks.append(current)
    return chunks


class RssService:
    def __init__(self, store: SqliteStore) -> None:
        self.store = store

    def refresh_all_feeds(self) -> int:
        total_new = 0
        feeds = self.store.list_all_active_feeds()
        for feed in feeds:
            try:
                parsed = feedparser.parse(feed.url)
                parsed_feed = cast(Any, getattr(parsed, "feed", None))
                feed_title = parsed_feed.get("title") if parsed_feed else None
                if feed_title:
                    self.store.upsert_feed(feed.url, title=str(feed_title))

                entries = [parse_entry(e) for e in parsed.entries]
                db_feed_id = feed.db_id if feed.db_id is not None else feed.id
                new_ids = self.store.insert_items(db_feed_id, entries)
                total_new += len(new_ids)
                self.store.update_feed_status(db_feed_id, error=None)
            except Exception as exc:
                logger.exception("Failed to refresh feed %s", feed.url)
                db_feed_id = feed.db_id if feed.db_id is not None else feed.id
                self.store.update_feed_status(db_feed_id, error=str(exc))
        return total_new

    async def push_recommendations(self, context: ContextTypes.DEFAULT_TYPE, max_per_chat: int = 5) -> None:
        for chat_id in self.store.list_chat_ids():
            await self.push_recommendations_for_chat(context, chat_id=chat_id, max_per_chat=max_per_chat)

    async def push_recommendations_for_chat(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        max_per_chat: int = 5,
        include_sent: bool = False,
    ) -> list[FeedItem]:
        keywords = self.store.get_keywords(chat_id)
        interest_terms = dict(self.store.get_interest_terms(chat_id, limit=300))
        source_weights = self.store.get_source_preferences(chat_id)
        candidates = self.store.get_candidate_items_for_chat(chat_id, limit=100)
        scored = sorted(
            candidates,
            key=lambda item: (rank_item(item, keywords, interest_terms, source_weights), item.published_at or ""),
            reverse=True,
        )

        sent_items: list[FeedItem] = []
        sent_count = 0
        for item in scored:
            if sent_count >= max_per_chat:
                break
            r = rank_item(item, keywords, interest_terms, source_weights)
            if keywords and r <= 0:
                continue

            text = (
                "[RSS推荐]\n"
                + format_item_summary(item, int(round(r)) if keywords else None)
                + "\n\n请评分（越高越感兴趣）："
            )
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=_build_feedback_keyboard(item.id),
            )
            if not include_sent:
                self.store.mark_sent(chat_id, item.id)
            sent_items.append(item)
            sent_count += 1

        return sent_items

    async def list_new_items_for_chat(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        max_items: int = 10,
    ) -> list[FeedItem]:
        keywords = self.store.get_keywords(chat_id)
        interest_terms = dict(self.store.get_interest_terms(chat_id, limit=300))
        source_weights = self.store.get_source_preferences(chat_id)
        candidates = self.store.get_candidate_items_for_chat(chat_id, limit=100)
        scored = sorted(
            candidates,
            key=lambda item: (rank_item(item, keywords, interest_terms, source_weights), item.published_at or ""),
            reverse=True,
        )

        selected: list[FeedItem] = []
        lines = ["[新鲜事列表]"]
        for item in scored[:max_items]:
            selected.append(item)
            lines.append(
                format_item_summary(
                    item,
                    int(round(rank_item(item, keywords, interest_terms, source_weights))) if keywords else None,
                )
            )

        if len(lines) == 1:
            return []

        for chunk in _chunk_message_lines(lines):
            await context.bot.send_message(chat_id=chat_id, text=chunk)
        return selected


async def rss_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    service: RssService = context.application.bot_data["rss_service"]
    new_count = service.refresh_all_feeds()
    logger.info("RSS refresh done. new_count=%s", new_count)
    now = datetime.now()
    if _in_push_window(now):
        await service.push_recommendations(context)
    else:
        logger.info("Skip push outside window: %s-%s", PUSH_WINDOW_START, PUSH_WINDOW_END)
