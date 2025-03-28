import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("single_video_scraper")

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
print("SUPABASE_URL from .env:", SUPABASE_URL)
print("SUPABASE_KEY from .env:", SUPABASE_KEY[:6] + "...")

# Connect to Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Set the external video storage folder (Documents/video library)
video_folder = os.path.expanduser("~/Documents/video library")
os.makedirs(video_folder, exist_ok=True)

# === Main function for a single URL ===
def scrape_video_from_url(video_url):
    logger.info(f"Scraping TikTok video from URL: {video_url}")

    ydl_opts = {
        "outtmpl": os.path.join(video_folder, "%(id)s.%(ext)s"),
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            ydl.download([video_url])
            logger.info("Video download complete.")
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return

    # Process downloaded metadata
    # Extract the video ID from the URL
    video_id = video_url.split("/")[-1]

    info_filename = f"{video_id}.info.json"
    filepath = os.path.join(video_folder, info_filename)

    if not os.path.exists(filepath):
        logger.error(f"Expected metadata file not found: {filepath}")
        return

    with open(filepath, "r") as f:
        data = json.load(f)

    try:
        if not video_id.isdigit():
            logger.info(f"Skipping non-video file: {info_filename}")
            return

        # ✳️ Check for duplicates
        existing = supabase.table("videos").select("id").eq("tiktok_id", video_id).execute()
        if existing.data:
            logger.info(f"Video {video_id} already exists in Supabase, skipping.")
            return

        best_format = next((f for f in data.get("formats", []) if f.get("vcodec") != "none"), {})

        row = {
            "tiktok_id": video_id,
            "creator_username": data.get("uploader", "unknown"),
            "video_url": data.get("webpage_url"),
            "title": data.get("title"),
            "description": data.get("description"),
            "upload_date": datetime.strptime(data.get("upload_date", "19700101"), "%Y%m%d").date().isoformat(),
            "views": data.get("view_count", 0),
            "likes": data.get("like_count", 0),
            "comments": data.get("comment_count", 0),
            "shares": data.get("share_count", 0),
            "saved": data.get("favorite_count", 0),
            "video_file": os.path.join(video_folder, f"{video_id}.mp4"),
            "video_download_url": best_format.get("url", ""),
            "resolution": f"{best_format.get('width', 0)}x{best_format.get('height', 0)}" if best_format.get("width") else None,
            "duration": data.get("duration", 0),
            "status": "downloaded"
        }

        logger.info(f"Inserting video {video_id} into Supabase...")
        supabase.table("videos").insert(row).execute()

    except Exception as e:
        logger.error(f"Failed to process {info_filename}: {e}")
# === Entry point ===
if __name__ == "__main__":
    # 🧠 Replace this with the specific TikTok video you want to download:
    scrape_video_from_url("https://www.tiktok.com/@unseriousprof/video/7167154950231870763")