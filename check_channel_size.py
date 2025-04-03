import os
import logging
from dotenv import load_dotenv
import yt_dlp

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tiktok_video_counter")

# Load env variables (optional, included for consistency)
load_dotenv()

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
    # Prompt user for channel name in terminal
    username = input("Enter TikTok username (e.g., hankgreen1): ").strip()
    if not username:
        logger.error("❌ No username provided. Exiting.")
    else:
        total_videos = get_channel_video_count(username)
        if total_videos is not None:
            logger.info(f"✅ Total videos for @{username}: {total_videos}")
        else:
            logger.info(f"❌ Could not determine video count for @{username}")