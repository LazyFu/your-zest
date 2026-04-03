import logging
import os
from datetime import time

from telegram import BotCommand, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from rss_service import RssService, rss_tick
from rss_store import SqliteStore


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def parse_keywords(raw: str) -> list[str]:
    chunks = raw.replace("，", ",").split(",")
    values: list[str] = []
    for chunk in chunks:
        values.extend(chunk.strip().split())
    return [v for v in values if v]


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except ValueError:
        return default


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            ["/addfeed", "/removefeed"],
            ["/listfeeds", "/removekeywords"],
            ["/sendnow", "/preview"],
            ["/setkeywords", "/mykeywords"],
            ["/schedule", "/stop"],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def _looks_like_rss_url(value: str) -> bool:
    text = value.strip()
    return text.startswith("http://") or text.startswith("https://")


def _build_help_text() -> str:
    return (
        "/start: 打开欢迎和快捷键盘\n"
        "/addfeed [rss_url]: 添加订阅（可先输入命令，再补链接）\n"
        "/listfeeds: 查看订阅列表\n"
        "/removefeed [feed_id]: 删除 feed 及相关条目\n"
        "/setkeywords <词1,词2>: 设置兴趣关键词\n"
        "/removekeywords <词1,词2>: 删除指定关键词\n"
        "/mykeywords: 查看兴趣关键词\n"
        "/sendnow [数量]: 立刻推送推荐\n"
        "/preview [数量]: 查看新鲜事列表\n"
        "/schedule: 每天 09:00 发送测试消息\n"
        "/stop: 停止测试定时消息\n"
        "自动推送频率: 每 30 分钟，仅 09:00-23:00 时段。\n"
        "评分可直接点击推荐消息下方按钮。"
    )


def _feedback_value_from_scale(scale: int, rating: int) -> int:
    if scale == 3:
        return {1: -2, 2: 0, 3: 2}.get(rating, 0)
    return {1: -2, 2: -1, 3: 0, 4: 1, 5: 2}.get(rating, 0)


def _feedback_label(scale: int, rating: int) -> str:
    if scale == 3:
        return {1: "不感兴趣", 2: "一般", 3: "感兴趣"}.get(rating, "一般")
    return {
        1: "很不感兴趣",
        2: "不感兴趣",
        3: "一般",
        4: "感兴趣",
        5: "很感兴趣",
    }.get(rating, "一般")


async def prompt_remove_feed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    store: SqliteStore = context.application.bot_data["rss_store"]
    feeds = store.list_subscriptions(update.effective_chat.id)
    if not feeds:
        await update.message.reply_text("你当前没有可删除的 feed。")
        return

    user_data = context.user_data
    if user_data is not None:
        user_data["awaiting_removefeed_id"] = True

    lines = ["请输入要删除的 feed 编号："]
    for feed in feeds:
        title = feed.title or "(no title)"
        lines.append(f"[{feed.id}] {title}")
    await update.message.reply_text("\n".join(lines))


async def remove_feed_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_feed_id: str) -> None:
    if not update.effective_chat or not update.message:
        return

    try:
        feed_id = int(raw_feed_id.strip())
    except ValueError:
        await update.message.reply_text("feed_id 必须是整数，请重新输入。")
        user_data = context.user_data
        if user_data is not None:
            user_data["awaiting_removefeed_id"] = True
        return

    store: SqliteStore = context.application.bot_data["rss_store"]
    feed = store.get_feed_by_display_no(feed_id)
    removed = store.delete_feed(feed_id)
    if removed and feed is not None:
        await update.message.reply_text(f"删除成功\n编号: {feed.id}\n链接: {feed.url}")
    else:
        await update.message.reply_text("没有找到该 feed。")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(
            "你好，我是 Telegram Bot。\n你可以用我来订阅 RSS、接收推荐并评分。",
            reply_markup=main_menu(),
        )
        await update.message.reply_text(_build_help_text())


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.text:
        text = update.message.text.strip()
        user_data = context.user_data
        if user_data is not None and user_data.get("awaiting_addfeed_url"):
            user_data.pop("awaiting_addfeed_url", None)
            await add_feed_from_url(update, context, text)
            return
        if user_data is not None and user_data.get("awaiting_removefeed_id"):
            user_data.pop("awaiting_removefeed_id", None)
            await remove_feed_by_id(update, context, text)
            return
        if text in {"addfeed", "removefeed"}:
            if text == "addfeed":
                await add_feed(update, context)
            else:
                await prompt_remove_feed(update, context)
            return
        await update.message.reply_text(update.message.text)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(_build_help_text())


async def send_now(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    service: RssService = context.application.bot_data["rss_service"]
    default_limit = int(context.application.bot_data.get("push_limit", 5))
    limit = default_limit
    if context.args:
        try:
            limit = max(1, int(context.args[0]))
        except ValueError:
            await update.message.reply_text("用法: /sendnow [数量]，例如 /sendnow 3")
            return

    refreshed = service.refresh_all_feeds()
    sent = await service.push_recommendations_for_chat(
        context,
        chat_id=update.effective_chat.id,
        max_per_chat=limit,
        include_sent=False,
    )
    if not sent:
        await update.message.reply_text(f"已刷新 {refreshed} 条，但当前没有可推送的新内容。")


async def preview_items(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    service: RssService = context.application.bot_data["rss_service"]
    default_limit = int(context.application.bot_data.get("preview_limit", 10))
    limit = default_limit
    if context.args:
        try:
            limit = max(1, int(context.args[0]))
        except ValueError:
            await update.message.reply_text("用法: /preview [数量]，例如 /preview 8")
            return

    service.refresh_all_feeds()
    shown = await service.list_new_items_for_chat(
        context,
        chat_id=update.effective_chat.id,
        max_items=limit,
    )
    if not shown:
        await update.message.reply_text("当前没有可展示的新内容。")


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "打开欢迎和命令键盘"),
            BotCommand("help", "查看命令说明"),
            BotCommand("addfeed", "添加 RSS 订阅"),
            BotCommand("listfeeds", "查看订阅列表"),
            BotCommand("removefeed", "删除订阅与相关条目"),
            BotCommand("setkeywords", "设置兴趣关键词"),
            BotCommand("removekeywords", "删除指定关键词"),
            BotCommand("mykeywords", "查看兴趣关键词"),
            BotCommand("sendnow", "立刻推送推荐"),
            BotCommand("preview", "查看新鲜事列表"),
            BotCommand("schedule", "开启测试定时消息"),
            BotCommand("stop", "停止测试定时消息"),
        ]
    )


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled bot error", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text("处理请求时出错了，我已经记录日志。")


async def feedback_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return

    data = query.data or ""
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer("评分数据无效", show_alert=True)
        return

    try:
        item_id = int(parts[1])
        scale = int(parts[2])
        rating = int(parts[3])
    except ValueError:
        await query.answer("评分数据无效", show_alert=True)
        return

    chat = update.effective_chat
    if chat is None:
        await query.answer()
        return

    store: SqliteStore = context.application.bot_data["rss_store"]
    label = _feedback_label(scale, rating)
    value = _feedback_value_from_scale(scale, rating)
    store.record_feedback(chat.id, item_id, label, value)

    await query.answer(f"已评分: {label}")


async def add_feed_from_url(update: Update, context: ContextTypes.DEFAULT_TYPE, url: str) -> None:
    if not update.effective_chat or not update.message:
        return

    user_data = context.user_data
    if not _looks_like_rss_url(url):
        await update.message.reply_text("RSS 链接必须以 http:// 或 https:// 开头，请重新发送。")
        if user_data is not None:
            user_data["awaiting_addfeed_url"] = True
        return

    store: SqliteStore = context.application.bot_data["rss_store"]
    feed = store.upsert_feed(url)
    store.subscribe(update.effective_chat.id, feed.id)
    await update.message.reply_text(f"添加成功\n编号: {feed.id}\n链接: {feed.url}")


async def add_feed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    if not context.args:
        user_data = context.user_data
        if user_data is not None:
            user_data["awaiting_addfeed_url"] = True
        await update.message.reply_text("请直接发送 RSS 链接，我会替你完成添加。")
        return

    await add_feed_from_url(update, context, context.args[0].strip())


async def list_feeds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    store: SqliteStore = context.application.bot_data["rss_store"]
    feeds = store.list_subscriptions(update.effective_chat.id)
    if not feeds:
        await update.message.reply_text("你还没有订阅任何 RSS。")
        return

    lines = ["你当前订阅的 RSS:"]
    for feed in feeds:
        title = feed.title or "(no title)"
        lines.append(f"[{feed.id}] {title} - {feed.url}")
    await update.message.reply_text("\n".join(lines))


async def remove_feed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    if not context.args:
        await prompt_remove_feed(update, context)
        return
    await remove_feed_by_id(update, context, context.args[0])


async def set_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    if not context.args:
        await update.message.reply_text("用法: /setkeywords 电影, 科技, 运动")
        return

    raw = " ".join(context.args)
    keywords = parse_keywords(raw)
    store: SqliteStore = context.application.bot_data["rss_store"]
    store.set_keywords(update.effective_chat.id, keywords)
    await update.message.reply_text(f"关键词已更新: {', '.join(keywords) if keywords else '(空)'}")


async def my_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    store: SqliteStore = context.application.bot_data["rss_store"]
    keywords = store.get_keywords(update.effective_chat.id)
    if not keywords:
        await update.message.reply_text("你还没有设置关键词，使用 /setkeywords 设置。")
        return
    await update.message.reply_text("你的关键词: " + ", ".join(keywords))


async def remove_keywords(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return
    if not context.args:
        await update.message.reply_text("用法: /removekeywords 词1,词2")
        return

    raw = " ".join(context.args)
    to_remove = parse_keywords(raw)
    store: SqliteStore = context.application.bot_data["rss_store"]
    updated = store.remove_keywords(update.effective_chat.id, to_remove)
    await update.message.reply_text(
        "关键词已更新: " + (", ".join(updated) if updated else "(空)")
    )


async def repeated_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    if job is None or job.chat_id is None:
        return

    await context.bot.send_message(
        chat_id=job.chat_id,
        text="定时消息：这是 bot 自动发送的消息。",
    )


async def schedule_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    job_queue = context.job_queue
    if job_queue is None:
        await update.message.reply_text("当前环境没有启用 JobQueue，无法设置定时消息。")
        return

    chat_id = update.effective_chat.id
    interval_minutes = int(context.application.bot_data.get("rss_poll_interval_minutes", 15))
    for job in job_queue.get_jobs_by_name(str(chat_id)):
        job.schedule_removal()

    job_queue.run_daily(
        repeated_message,
        time=time(hour=9, minute=0),
        chat_id=chat_id,
        name=str(chat_id),
    )
    await update.message.reply_text(
        f"已开始每天 09:00 定时发送。RSS 更新间隔当前为每 {interval_minutes} 分钟。"
    )


async def stop_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat or not update.message:
        return

    job_queue = context.job_queue
    if job_queue is None:
        await update.message.reply_text("当前环境没有启用 JobQueue。")
        return

    chat_id = update.effective_chat.id
    jobs = job_queue.get_jobs_by_name(str(chat_id))
    if not jobs:
        await update.message.reply_text("当前没有在运行的定时消息。")
        return

    for job in jobs:
        job.schedule_removal()
    await update.message.reply_text("已停止定时消息。")


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("请先设置环境变量 TELEGRAM_BOT_TOKEN")

    app = Application.builder().token(token).post_init(post_init).build()
    store = SqliteStore(os.getenv("RSS_DB_PATH", "rss_data.db"))
    service = RssService(store)
    app.bot_data["rss_store"] = store
    app.bot_data["rss_service"] = service
    app.bot_data["rss_poll_interval_minutes"] = env_int("RSS_POLL_INTERVAL_MINUTES", 30)
    app.bot_data["push_limit"] = env_int("RSS_PUSH_LIMIT", 5)
    app.bot_data["preview_limit"] = env_int("RSS_PREVIEW_LIMIT", 10)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("addfeed", add_feed))
    app.add_handler(CommandHandler("listfeeds", list_feeds))
    app.add_handler(CommandHandler("removefeed", remove_feed))
    app.add_handler(CommandHandler("setkeywords", set_keywords))
    app.add_handler(CommandHandler("removekeywords", remove_keywords))
    app.add_handler(CommandHandler("mykeywords", my_keywords))
    app.add_handler(CommandHandler("sendnow", send_now))
    app.add_handler(CommandHandler("preview", preview_items))
    app.add_handler(CommandHandler("schedule", schedule_message))
    app.add_handler(CommandHandler("stop", stop_message))
    app.add_handler(CallbackQueryHandler(feedback_callback, pattern=r"^fb:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))
    app.add_error_handler(on_error)

    if app.job_queue:
        poll_interval = int(app.bot_data["rss_poll_interval_minutes"])
        app.job_queue.run_repeating(
            rss_tick,
            interval=60 * poll_interval,
            first=5,
            name="rss-poller",
        )
        logger.info("RSS polling interval set to %s minutes", poll_interval)
    else:
        logger.warning("JobQueue not available; RSS polling disabled.")

    logger.info("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
