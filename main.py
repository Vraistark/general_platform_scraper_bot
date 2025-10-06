import asyncio
from scrapers import youtube_scraper, tikwm_scraper, dailymotion_scraper, okru_scraper, extract_domain

async def main():
    # Example URLs per platform
    youtube_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        # Add your test URLs
    ]

    tiktok_urls = [
        "https://www.tiktok.com/@someuser/video/1234567890",
        # Add your test URLs
    ]

    dailymotion_urls = [
        "https://www.dailymotion.com/video/x7yuykq",
        # Add your test URLs
    ]

    okru_urls = [
        "https://ok.ru/video/1234567890123",
        # Add your test URLs
    ]

    print("Scraping YouTube...")
    youtube_data = await youtube_scraper(youtube_urls)
    print(youtube_data)

    print("Scraping TikTok...")
    tiktok_data = await tikwm_scraper(tiktok_urls)
    print(tiktok_data)

    print("Scraping Dailymotion...")
    dailymotion_data = await dailymotion_scraper(dailymotion_urls)
    print(dailymotion_data)

    print("Scraping Ok.ru...")
    okru_data = await okru_scraper(okru_urls)
    print(okru_data)

if __name__ == "__main__":
    asyncio.run(main())
