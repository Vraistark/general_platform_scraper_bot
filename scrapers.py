import re
import asyncio
import aiohttp # type: ignore
from datetime import datetime

# -- YouTube Scraper --

YOUTUBE_API_KEYS = [
    "AIzaSyAuO4RUHSiW9blpNMfyuM2zY7PMQsN4hZk",
    "AIzaSyAHyyTn7dbxl_a0JoqWKGparSvVgDJV1bw",
    "AIzaSyCMEZ-mR1570vfHdj9K49T1KqKmPs-JQB0"
]

_youtube_key_index = 0

def _rotate_youtube_key():
    global _youtube_key_index
    key = YOUTUBE_API_KEYS[_youtube_key_index]
    _youtube_key_index = (_youtube_key_index + 1) % len(YOUTUBE_API_KEYS)
    return key

def extract_youtube_video_id(url: str):
    try:
        decoded_url = url.strip()
        regex = r"(?:v=|\/shorts\/|\/live\/|\.be\/|\/embed\/|\/watch\?v=|\/watch\?.*?v=)([a-zA-Z0-9_-]{11})"
        match = re.search(regex, decoded_url)
        if match:
            return match.group(1)
        else:
            return None
    except:
        return None

def format_duration_ISO8601(duration):
    import re
    hours, minutes, seconds = 0, 0, 0
    match = re.match(r"PT((\d+)H)?((\d+)M)?((\d+)S)?", duration)
    if not match:
        return "00:00:00"
    if match.group(2):
        hours = int(match.group(2))
    if match.group(4):
        minutes = int(match.group(4))
    if match.group(6):
        seconds = int(match.group(6))
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

async def fetch_youtube_videos(video_ids):
    api_key = _rotate_youtube_key()
    ids_str = ",".join(video_ids)
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&id={ids_str}&key={api_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.json()

async def fetch_youtube_channels(channel_ids):
    api_key = _rotate_youtube_key()
    ids_str = ",".join(channel_ids)
    url = f"https://www.googleapis.com/youtube/v3/channels?part=snippet,statistics&id={ids_str}&key={api_key}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.json()

async def youtube_scraper(urls):
    video_ids = []
    for url in urls:
        vid = extract_youtube_video_id(url)
        if vid and vid not in video_ids:
            video_ids.append(vid)

    if not video_ids:
        return []

    results = []
    CHUNK_SIZE = 50
    for i in range(0, len(video_ids), CHUNK_SIZE):
        chunk = video_ids[i:i+CHUNK_SIZE]
        video_data = await fetch_youtube_videos(chunk)
        if not video_data or "items" not in video_data:
            continue
        channel_ids = [v["snippet"]["channelId"] for v in video_data["items"]]
        channel_data = await fetch_youtube_channels(channel_ids)
        channel_map = {}
        if channel_data and "items" in channel_data:
            for ch in channel_data["items"]:
                channel_map[ch["id"]] = {
                    "name": ch["snippet"]["title"],
                    "subs": ch["statistics"].get("subscriberCount", "0"),
                    "username": "@" + ch["snippet"].get("customUrl", "").lstrip("@")
                }
        for video in video_data["items"]:
            channel_id = video["snippet"]["channelId"]
            details = video.get("contentDetails", {})
            duration_fmt = format_duration_ISO8601(details.get("duration", "")) if "duration" in details else "00:00:00"
            stats = video.get("statistics", {})
            results.append({
                "source_url": f"https://www.youtube.com/watch?v={video['id']}",
                "title": video["snippet"].get("title", ""),
                "videoId": video["id"],
                "views": stats.get("viewCount", "0"),
                "duration": duration_fmt,
                "channelId": channel_id,
                "channel_name": channel_map.get(channel_id, {}).get("name", ""),
                "channel_subs": channel_map.get(channel_id, {}).get("subs", "0"),
                "likes": stats.get("likeCount", "0"),
                "publish_date": video["snippet"].get("publishedAt", "").split("T")[0],
                "channel_username": channel_map.get(channel_id, {}).get("username", ""),
            })
    return results


# -- TikTok Scraper --

TIKWM_API = 'https://www.tikwm.com/api/'

def format_duration_tiktok(seconds):
    hrs = seconds // 3600
    mins = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hrs:02d}:{mins:02d}:{secs:02d}"

def format_date_tiktok(ts_ms):
    dt = datetime.fromtimestamp(ts_ms/1000)
    return dt.strftime("%H:%M:%S %d-%m-%Y")

async def tikwm_scraper(urls):
    results = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                api_url = f"{TIKWM_API}?url={url}"
                async with session.get(api_url) as resp:
                    if resp.status != 200:
                        results.append({
                            "source_url": url,
                            "title": "N/A",
                            "views": "N/A",
                            "duration": "00:00:00",
                            "likes": "N/A",
                            "comments": "N/A",
                            "upload_date": "N/A",
                            "profile_url": "N/A",
                            "author_name": "N/A",
                            "subscribers": "N/A",
                            "channel_username": "N/A"
                        })
                        continue
                    data = await resp.json()
                    if not data.get("data"):
                        results.append({
                            "source_url": url,
                            "title": "N/A",
                            "views": "N/A",
                            "duration": "00:00:00",
                            "likes": "N/A",
                            "comments": "N/A",
                            "upload_date": "N/A",
                            "profile_url": "N/A",
                            "author_name": "N/A",
                            "subscribers": "N/A",
                            "channel_username": "N/A"
                        })
                        continue
                    d = data["data"]
                    author = d.get("author", {})
                    results.append({
                        "source_url": url,
                        "title": d.get("title", "N/A"),
                        "views": d.get("play_count", "N/A"),
                        "duration": format_duration_tiktok(d.get("duration", 0)),
                        "likes": d.get("digg_count", "N/A"),
                        "comments": d.get("comment_count", "N/A"),
                        "upload_date": format_date_tiktok(d.get("create_time", 0) * 1000),
                        "profile_url": f"https://www.tiktok.com/@{author.get('unique_id')}" if author.get("unique_id") else "N/A",
                        "author_name": author.get("nickname", "N/A"),
                        "subscribers": author.get("follower_count", "N/A"),
                        "channel_username": author.get("nickname", "N/A")  # Assuming username same as author name
                    })
            except Exception:
                results.append({
                    "source_url": url,
                    "title": "Error",
                    "views": "Error",
                    "duration": "00:00:00",
                    "likes": "Error",
                    "comments": "Error",
                    "upload_date": "Error",
                    "profile_url": "Error",
                    "author_name": "Error",
                    "subscribers": "Error",
                    "channel_username": "Error"
                })
    return results


# -- Dailymotion Scraper --

async def dailymotion_scraper(urls):
    results = []
    api_base = 'https://api.dailymotion.com/video/'

    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                video_id = re.sub(r'https://www\.dailymotion\.com/video/', '', url)
                video_url = api_base + video_id + '?fields=id,title,created_time,duration,views_total,likes_total,owner'
                async with session.get(video_url) as resp:
                    if resp.status != 200:
                        results.append({
                            "source_url": url,
                            "title": "N/A",
                            "upload_date": "N/A",
                            "duration": "00:00:00",
                            "views": "N/A",
                            "likes": "N/A",
                            "channel_name": "N/A",
                            "channel_url": "N/A",
                            "subscribers": "N/A",
                            "channel_username": "N/A"
                        })
                        continue
                    video_data = await resp.json()

                created_time = video_data.get("created_time", None)
                upload_date = datetime.utcfromtimestamp(created_time).strftime('%Y-%m-%d') if created_time else "N/A"

                owner_id = video_data.get("owner", "")
                channel_name = "N/A"
                channel_url = "N/A"
                if owner_id:
                    user_api = f'https://api.dailymotion.com/user/{owner_id}?fields=username,url'
                    async with session.get(user_api) as owner_resp:
                        if owner_resp.status == 200:
                            owner_data = await owner_resp.json()
                            channel_name = owner_data.get("username", "N/A")
                            channel_url = owner_data.get("url", "N/A")

                duration_sec = video_data.get("duration", 0)
                hours = int(duration_sec // 3600)
                minutes = int((duration_sec % 3600) // 60)
                seconds = int(duration_sec % 60)
                duration_fmt = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                results.append({
                    "source_url": url,
                    "title": video_data.get("title", "N/A"),
                    "upload_date": upload_date,
                    "duration": duration_fmt,
                    "views": video_data.get("views_total", "N/A"),
                    "likes": video_data.get("likes_total", "N/A"),
                    "channel_name": channel_name,
                    "channel_url": channel_url,
                    "subscribers": "N/A",  # Field not available from API
                    "channel_username": channel_name  # Using channel_name here
                })
            except Exception:
                results.append({
                    "source_url": url,
                    "title": "Error",
                    "upload_date": "Error",
                    "duration": "00:00:00",
                    "views": "Error",
                    "likes": "Error",
                    "channel_name": "Error",
                    "channel_url": "Error",
                    "subscribers": "Error",
                    "channel_username": "Error"
                })
    return results


# -- ok.ru Scraper --

async def okru_scraper(urls):
    results = []
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        results.append({
                            "source_url": url,
                            "title": "N/A",
                            "duration": "00:00:00",
                            "views": "N/A",
                            "channel_url": "N/A",
                            "channel_name": "N/A",
                            "subscribers": "N/A",
                            "upload_date": "N/A",
                            "likes": "N/A", 
                            "channel_username": "N/A"
                        })
                        continue
                    text = await resp.text()

                    def re_search(pattern):
                        m = re.search(pattern, text, re.IGNORECASE)
                        return m.group(1) if m else "N/A"

                    title = re_search(r'<meta property="og:title" content="([^"]+)"')
                    duration_str = re_search(r'class="vid-card_duration">([\d:]+)<\/div>')
                    duration_parts = duration_str.split(":") if duration_str != "N/A" else []
                    if len(duration_parts) == 3:
                        h, m, s = map(int, duration_parts)
                        duration = f"{h:02d}:{m:02d}:{s:02d}"
                    elif len(duration_parts) == 2:
                        m, s = map(int, duration_parts)
                        duration = f"00:{m:02d}:{s:02d}"
                    else:
                        duration = "00:00:00"

                    upload_date = re_search(r'<meta property="video:release_date" content="([^"]+)"')
                    if upload_date == "N/A":
                        upload_date = re_search(r'"datePublished":"([^"]+)"')

                    views = re_search(r'<div class="vp-layer-info_i"><span>([^<]+)<\/span>')
                    channel_url = re_search(r'\/(group|profile)\/([\w\d]+)')
                    channel_url = f"https://ok.ru/{channel_url}" if channel_url != "N/A" else "N/A"
                    channel_name = re_search(r'name="([^"]+)" id="\d+"')
                    subscribers = re_search(r'subscriberscount="(\d+)"')

                    results.append({
                        "source_url": url,
                        "title": title,
                        "duration": duration,
                        "views": views,
                        "channel_url": channel_url,
                        "channel_name": channel_name,
                        "subscribers": subscribers,
                        "upload_date": upload_date,
                        "likes": "N/A",
                        "channel_username": channel_name
                    })
            except Exception:
                results.append({
                    "source_url": url,
                    "title": "Error",
                    "duration": "00:00:00",
                    "views": "Error",
                    "channel_url": "Error",
                    "channel_name": "Error",
                    "subscribers": "Error",
                    "upload_date": "Error",
                    "likes": "Error",
                    "channel_username": "Error"
                })
    return results

# -- Domain Extractor --

def extract_domain(url):
    try:
        regex = r'^(?:https?:\/\/)?(?:www\.)?([^\/\n?]+)'
        match = re.match(regex, url)
        if match:
            return match.group(1)
    except:
        pass
    return ""
