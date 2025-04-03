import logging
import yt_dlp
from urllib.parse import urlparse, parse_qs

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tiktok_video_counter")

def extract_username(url):
    """
    Extracts the TikTok username from a URL, removing query parameters.
    
    Args:
        url (str): TikTok URL (e.g., https://www.tiktok.com/@username?_t=abc&_r=1)
    
    Returns:
        str: Username (e.g., "username"), or None if invalid
    """
    try:
        parsed = urlparse(url.strip())
        if parsed.netloc != "www.tiktok.com":
            return None
        path = parsed.path.strip("/")
        if not path.startswith("@"):
            return None
        return path.split("@")[1].split("?")[0]  # Get username, drop query params
    except Exception:
        return None

def get_channel_video_count(username):
    """
    Fetches the total number of videos posted by a TikTok channel.
    
    Args:
        username (str): The TikTok username (e.g., "hankgreen1")
    
    Returns:
        int: Total number of videos, or None if failed
    """
    logger.info(f"🔍 Checking video count for @{username}")

    ydl_opts = {
        "extract_flat": True,  # Only fetch metadata, don't download
        "quiet": True,        # Suppress yt_dlp output
        "no_warnings": True,  # Suppress warnings
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            url = f"https://www.tiktok.com/@{username}"
            info = ydl.extract_info(url, download=False)
            
            if "entries" not in info:
                logger.error(f"❌ No video entries found for @{username}. Channel might be private or invalid.")
                return None
            
            video_count = len(info["entries"])
            logger.info(f"📊 @{username} has {video_count} videos")
            return video_count

    except Exception as e:
        logger.error(f"⚠️ Failed to fetch video count for @{username}: {e}")
        return None

# === Run script ===
if __name__ == "__main__":
    logger.info("Enter TikTok URLs (one per line). Press Ctrl+D (Unix) or Ctrl+Z (Windows) then Enter when done:")
    urls = []
    try:
        while True:
            line = input().strip()
            if line:
                urls.append(line)
    except EOFError:
        pass  # User finished input with Ctrl+D or Ctrl+Z

    if not urls:
        logger.error("❌ No URLs provided. Exiting.")
    else:
        video_counts = []
        for url in urls:
            username = extract_username(url)
            if not username:
                logger.error(f"❌ Invalid URL format: {url}")
                video_counts.append(None)
                continue
            
            total_videos = get_channel_video_count(username)
            video_counts.append(total_videos)
            if total_videos is not None:
                logger.info(f"✅ Total videos for @{username}: {total_videos}")
            else:
                logger.info(f"❌ Could not determine video count for @{username}")

        # Output the list of video counts
        logger.info("\nVideo counts in order (copy this list):")
        print("\n".join(str(count) if count is not None else "N/A" for count in video_counts))