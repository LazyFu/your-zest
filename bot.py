import logging
import os
from datetime import time

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    if update.message:
        await update.message.reply_text("你好，我是 Telegram Bot。发我任意文本，我会原样回复。")


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Echo user text messages."""
    if update.message and update.message.text:
        await update.message.reply_text(update.message.text)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command."""
    if update.message:
        await update.message.reply_text(
            "/start: 打招呼\n"
            "/schedule: 每天定时发送消息\n"
            "/stop: 停止定时消息\n"
            "发我任意文本，我会原样回复。"
        )

async def repeated_message(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Send a scheduled message."""
    job = context.job
    if job is None or job.chat_id is None:
        return

    await context.bot.send_message(
        chat_id=job.chat_id,
        text="定时消息：这是 bot 自动发送的消息。",
    )


async def schedule_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Start sending a message every day at a fixed time."""
    if not update.effective_chat or not update.message:
        return

    job_queue = context.job_queue
    if job_queue is None:
        await update.message.reply_text("当前环境没有启用 JobQueue，无法设置定时消息。")
        return

    chat_id = update.effective_chat.id
    for job in job_queue.get_jobs_by_name(str(chat_id)):
        job.schedule_removal()

    # job_queue.run_repeating(
    #     repeated_message,
    #     interval=60,
    #     first=0,
    #     chat_id=chat_id,
    #     name=str(chat_id),
    # )

    job_queue.run_daily(
        repeated_message,
        time=time(hour=9, minute=0),
        chat_id=chat_id,
        name=str(chat_id),
    )
    await update.message.reply_text("已开始每天 09:00 定时发送。")


async def stop_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stop scheduled messages for this chat."""
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

    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command)) 
    app.add_handler(CommandHandler("schedule", schedule_message))
    app.add_handler(CommandHandler("stop", stop_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, echo))

    logger.info("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()
