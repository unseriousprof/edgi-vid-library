import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("channel_scraper")

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

video_folder = os.path.expanduser("~/Documents/video_tmp")
os.makedirs(video_folder, exist_ok=True)

def scrape_tiktok_channel(username):
    logger.info(f"Scraping TikTok channel: @{username}")

    ydl_opts = {
        "outtmpl": os.path.join(video_folder, "%(id)s.%(ext)s"),
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            ydl.download([f"https://www.tiktok.com/@{username}"])
        except Exception as e:
            logger.error(f"Failed to scrape @{username}: {e}")
            return

    # === Process .info.json metadata files ===
    for filename in os.listdir(video_folder):
        if not filename.endswith(".info.json"):
            continue

        filepath = os.path.join(video_folder, filename)

        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            video_id = data.get("id")

            # 🧠 Skip playlist-level or non-video metadata
            if not video_id or not video_id.isdigit():
                logger.info(f"Skipping non-video metadata file: {filename}")
                os.remove(filepath)
                continue

            video_filename = f"{video_id}.mp4"
            local_path = os.path.join(video_folder, video_filename)

            # Skip if already in Supabase
            exists = supabase.table("videos").select("id").eq("tiktok_id", video_id).execute()
            if exists.data:
                logger.info(f"Video {video_id} already exists, skipping.")
                os.remove(filepath)
                if os.path.exists(local_path):
                    os.remove(local_path)
                continue

            # Upload video to Supabase storage
            with open(local_path, "rb") as file:
                supabase.storage.from_("videos").upload(f"{video_id}.mp4", file)

            public_url = f"{SUPABASE_URL.replace('.co', '.co/storage/v1/object/public')}/videos/{video_id}.mp4"

            row = {
                "tiktok_id": video_id,
                "creator_username": username,
                "video_url": data.get("webpage_url"),
                "title": data.get("title"),
                "description": data.get("description"),
                "upload_date": datetime.strptime(data.get("upload_date", "19700101"), "%Y%m%d").date().isoformat(),
                "views": data.get("view_count", 0),
                "likes": data.get("like_count", 0),
                "comments": data.get("comment_count", 0),
                "shares": data.get("share_count", 0),
                "saved": data.get("favorite_count", 0),
                "video_file": public_url,
                "video_download_url": data.get("url", ""),
                "resolution": f"{data.get('width', 0)}x{data.get('height', 0)}" if data.get("width") else None,
                "duration": data.get("duration", 0),
                "status": "uploaded"
            }

            supabase.table("videos").insert(row).execute()
            logger.info(f"✅ Inserted video {video_id} from @{username}")

            # Clean up
            os.remove(filepath)
            os.remove(local_path)

        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}")

# === Entry point ===
if __name__ == "__main__":
    scrape_tiktok_channel("spaceiac")