import os
import io
import logging
import re
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters
)
import openpyxl # type: ignore
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

TEMPLATE_FILES = {
    "YouTube": "YouTube-Template.xlsx",
    "TikTok": "UGC-Template.xlsx",
    "Dailymotion": "UGC-Template.xlsx",
    "Ok.ru": "UGC-Template.xlsx",
}

SELECT_PLATFORM, GET_INPUT = range(2)

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

fields_mapping = {
    "YouTube": {
        "source_url": "A",
        "title": "E",
        "videoId": "F",
        "views": "G",
        "duration": "H",
        "channelId": "I",
        "channel_name": "J",
        "channel_subs": "K",
        "likes": "L",
        "publish_date": "M",
        "channel_username": "T",
    },
    "TikTok": {
        "source_url": "A",
        "title": "B",
        "views": "C",
        "duration": "D",
        "likes": "F",
        "comments": "G",
        "upload_date": "H",
        "profile_url": "L",
        "author_name": "M",
        "subscribers": "N",
        "channel_username": "V",
    },
    "Dailymotion": {
        "source_url": "A",
        "title": "B",
        "views": "C",
        "duration": "D",
        "likes": "F",
        "upload_date": "H",
        "channel_url": "L",
        "channel_name": "M",
        "subscribers": "N",
        "channel_username": "V",
    },
    "Ok.ru": {
        "source_url": "A",
        "title": "B",
        "views": "C",
        "duration": "D",
        "likes": "F",
        "upload_date": "H",
        "channel_url": "L",
        "channel_name": "M",
        "subscribers": "N",
        "channel_username": "V",
    }
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
        f"You selected {platform}.\nSend me URLs line-by-line or upload an Excel file with URLs."
    )
    return GET_INPUT

def extract_urls_from_excel(file_bytes):
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb.active
    urls = []
    url_col_idx = None
    for cell in ws[1]:
        if cell.value and "url" in str(cell.value).lower():
            url_col_idx = cell.column - 1  # zero based index
            break
    if url_col_idx is None:
        return []
    for row in ws.iter_rows(min_row=2, values_only=True):
        val = row[url_col_idx]
        if val:
            urls.append(str(val))
    return urls

async def input_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    platform = context.user_data.get('platform')
    scraper_func = PLATFORMS.get(platform)

    urls = []
    if update.message.document:
        doc = update.message.document
        if not doc.file_name.lower().endswith(('.xls', '.xlsx')):
            await update.message.reply_text("Please upload an Excel file (.xls or .xlsx).")
            return GET_INPUT
        file_obj = await doc.get_file()
        file_bytes = await file_obj.download_as_bytearray()
        urls = extract_urls_from_excel(file_bytes)
        if not urls:
            await update.message.reply_text("Could not extract URLs from Excel file.")
            return GET_INPUT
    else:
        urls = [url.strip() for url in update.message.text.strip().splitlines() if url.strip()]

    if platform != "Domain Extractor":
        pattern = URL_PATTERNS.get(platform)
        invalid_urls = [u for u in urls if not pattern.match(u)]
        if invalid_urls:
            await update.message.reply_text(
                "Invalid URLs:\n" + "\n".join(invalid_urls) + "\nPlease send valid URLs."
            )
            return GET_INPUT

    await update.message.reply_text(f"Scraping {len(urls)} URLs for {platform}...")

    if platform == "Domain Extractor":
        rows = [{"URL": url, "Domain": extract_domain(url)} for url in urls]
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["URL", "Domain"])
        writer.writeheader()
        writer.writerows(rows)
        output.seek(0)
        await update.message.reply_document(document=output, filename="domains.csv")
        return ConversationHandler.END

    try:
        results = await scraper_func(urls)
    except Exception as e:
        logger.exception(f"Error scraping {platform}: {e}")
        await update.message.reply_text(f"Error during scraping: {e}")
        return ConversationHandler.END

    if not results:
        await update.message.reply_text("No data scraped.")
        return ConversationHandler.END

    template_path = TEMPLATE_FILES.get(platform)
    if not template_path or not os.path.isfile(template_path):
        await update.message.reply_text("Template file missing for this platform.")
        return ConversationHandler.END

    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    mapping = fields_mapping.get(platform)
    if not mapping:
        await update.message.reply_text("No field mapping found for this platform.")
        return ConversationHandler.END

    start_row = 2
    for i, row_data in enumerate(results, start=start_row):
        for field, col in mapping.items():
            val = row_data.get(field, "N/A")
            ws[f"{col}{i}"].value = val

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    file_name = f"{platform.replace(' ', '_')}_scraped_output.xlsx"
    await update.message.reply_document(document=output, filename=file_name)
    await update.message.reply_text("Here is your Excel file. Use /start to scrape again!")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

def main():
    TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    if not TOKEN:
        logger.error("Missing TELEGRAM_BOT_TOKEN")
        return

    bot = Bot(token=TOKEN)
    try:
        bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass

    app = ApplicationBuilder().token(TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            SELECT_PLATFORM: [CallbackQueryHandler(platform_selected)],
            GET_INPUT: [MessageHandler((filters.TEXT | filters.Document.File) & ~filters.COMMAND, input_received)]
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )

    app.add_handler(conv)
    logger.info("Bot is running")
    app.run_polling(allowed_updates=None)

if __name__ == "__main__":
    main()
