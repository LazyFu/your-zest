# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A Telegram bot that aggregates RSS feeds and pushes personalized recommendations. Users subscribe to feeds, set interest keywords, and rate items — feedback shapes future ranking via weighted term/source preferences stored in SQLite.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the bot
python bot.py
```

No test suite, linter, or type checker is configured.

## Architecture

Three modules, layered bottom-up:

- **`rss_store.py`** — SQLite data layer. Manages all tables (feeds, subscriptions, items, preferences, feedback, interest terms, source preferences, sent items) and migrations. Deduplicates items by canonical URL (stripping tracking params) and content hash. Thread-safe via `threading.Lock`.
- **`rss_service.py`** — Business logic. `RssService` refreshes feeds via `feedparser`, scores/ranks items using a composite formula (keyword hits, learned interest terms, source weight, freshness decay), and pushes recommendations to Telegram chats. The `rss_tick` coroutine is the recurring poller entry point.
- **`bot.py`** — Telegram bot using `python-telegram-bot`. Wires up command handlers, inline feedback callbacks, and the recurring RSS poll job. Stores `SqliteStore` and `RssService` instances in `app.bot_data`.

## Key design details

- **Display vs. DB IDs**: Feeds have a user-facing `display_no` separate from the internal `id`. All user-facing commands use `display_no`.
- **Deduplication**: `insert_items` checks both `canonical_link` (URL minus UTM/tracking params) and `content_hash` (SHA-256 of title+summary+content) to mark duplicates.
- **Ranking formula** (`rank_item` in `rss_service.py`): `keyword_score * 1.4 + interest_score + source_score + freshness_score`, where freshness decays exponentially with a 36-hour half-life.
- **Push window**: Automatic pushes only happen 09:00–23:00 (`PUSH_WINDOW_START` / `PUSH_WINDOW_END`). Manual `/sendnow` always works.
- **Feedback flow**: Inline keyboard buttons produce `fb:<item_id>:<scale>:<rating>` callback data. `record_feedback` updates source preferences and interest term weights based on the delta from any previous rating.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | (required) | Bot token from @BotFather |
| `RSS_DB_PATH` | `rss_data.db` | SQLite database path |
| `RSS_POLL_INTERVAL_MINUTES` | `30` | RSS refresh interval |
| `RSS_PUSH_LIMIT` | `5` | Max items per auto-push |
| `RSS_PREVIEW_LIMIT` | `10` | Max items for `/preview` |
