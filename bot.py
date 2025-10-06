import os
import io
import csv
import logging
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)
from scrapers import youtube_scraper, tikwm_scraper, dailymotion_scraper, okru_scraper, extract_domain

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PLATFORMS = {
    "YouTube": youtube_scraper,
    "TikTok": tikwm_scraper,
    "Dailymotion": dailymotion_scraper,
    "Ok.ru": okru_scraper,
    "Domain Extractor": None,
}

SELECT_PLATFORM, GET_URLS = range(2)

URL_PATTERNS = {
    "YouTube": re.compile(
        r"^(https?:\/\/)?(www\.)?((youtube\.com\/watch\?v=)|youtube\.com\/shorts\/|youtu\.be\/)[a-zA-Z0-9_-]{11}($|&|/|\?)"
    ),
    "TikTok": re.compile(
        r"^(https?:\/\/)?(www\.)?tiktok\.com\/@[\w._-]+\/video\/\d+"
    ),
    "Dailymotion": re.compile(
        r"^(https?:\/\/)?(www\.)?dailymotion\.com\/video\/[a-zA-Z0-9]+$"
    ),
    "Ok.ru": re.compile(
        r"^(https?:\/\/)?(www\.)?ok\.ru\/video\/\d+$"
    ),
    "Domain Extractor": re.compile(r".*"),  # Accept any format
}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton(name, callback_data=name)] for name in PLATFORMS]
    await update.message.reply_text(
        "Welcome! Please choose a platform to scrape:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return SELECT_PLATFORM

async def platform_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    platform = query.data
    context.user_data['platform'] = platform
    await query.edit_message_text(
        f"Send me the list of URLs (one per line) for {platform}:"
    )
    return GET_URLS

async def urls_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    platform_name = context.user_data.get('platform')
    scraper_func = PLATFORMS.get(platform_name)
    urls = [url.strip() for url in update.message.text.strip().splitlines() if url.strip()]

    # Validate URLs against platform regex
    pattern = URL_PATTERNS.get(platform_name)
    invalid_urls = [url for url in urls if not pattern.match(url)]

    if invalid_urls:
        await update.message.reply_text(
            f"The following URLs are invalid for {platform_name}:\n"
            + "\n".join(invalid_urls)
            + "\nPlease send valid URLs only."
        )
        return GET_URLS

    await update.message.reply_text(f"Processing {len(urls)} URLs for {platform_name}... Please wait.")

    # Special case for domain extractor
    if platform_name == "Domain Extractor":
        rows = [{"URL": url, "Domain": extract_domain(url)} for url in urls]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["URL", "Domain"])
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)
        await update.message.reply_document(document=output, filename="domains.csv")
        return ConversationHandler.END

    # For all platform scrapers
    try:
        results = await scraper_func(urls)
    except Exception as e:
        logger.exception(f"Error scraping {platform_name}:")
        await update.message.reply_text(f"Error during scraping: {e}")
        return ConversationHandler.END

    if not results:
        await update.message.reply_text("No data was scraped from the provided URLs.")
        return ConversationHandler.END

    # CSV formatting
    first = results[0]
    if isinstance(first, dict):
        headers = list(first.keys())
        data_rows = results
        writer_func = csv.DictWriter
    else:
        headers = [f"Field{i+1}" for i in range(len(first))]
        data_rows = results
        writer_func = csv.writer

    output = io.StringIO()
    if writer_func == csv.DictWriter:
        writer = writer_func(output, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data_rows)
    else:
        writer = writer_func(output)
        writer.writerow(headers)
        writer.writerows(data_rows)
    output.seek(0)

    await update.message.reply_document(document=output, filename=f"{platform_name.replace(' ', '_')}_output.csv")
    await update.message.reply_text("Here is your CSV file. Use /start to scrape again!")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

def main():
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        logger.error("Missing TELEGRAM_BOT_TOKEN environment variable.")
        return

    # Create bot instance and ensure no webhook is set
    bot = Bot(token=TOKEN)
    try:
        # Try to delete webhook before starting polling (ignore errors)
        bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.info("Webhook deletion not required or failed gracefully.")

    app = ApplicationBuilder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            SELECT_PLATFORM: [CallbackQueryHandler(platform_selected)],
            GET_URLS: [MessageHandler(filters.TEXT & ~filters.COMMAND, urls_received)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )

    app.add_handler(conv)
    logger.info("Bot is running!")
    app.run_polling(allowed_updates=None)

if __name__ == "__main__":
    main()
