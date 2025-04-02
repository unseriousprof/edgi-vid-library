import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("supa_single_vid_scraper")

# Load env variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Temp folder for local download
video_folder = os.path.expanduser("~/Documents/video_tmp")
os.makedirs(video_folder, exist_ok=True)

def scrape_single_video(video_url):
    logger.info(f"Scraping video: {video_url}")

    ydl_opts = {
        "outtmpl": os.path.join(video_folder, "%(id)s.%(ext)s"),
        "writeinfojson": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            ydl.download([video_url])
        except Exception as e:
            logger.error(f"Download failed: {e}")
            return

    # Process downloaded .info.json
    for filename in os.listdir(video_folder):
        if filename.endswith(".info.json"):
            filepath = os.path.join(video_folder, filename)
            with open(filepath, "r") as f:
                data = json.load(f)

            try:
                video_id = data.get("id")
                video_filename = f"{video_id}.mp4"
                local_path = os.path.join(video_folder, video_filename)

                # Upload to Supabase Storage
                with open(local_path, "rb") as file:
                    storage_path = f"{video_id}.mp4"
                    supabase.storage.from_("videos").upload(storage_path, file)

                public_url = f"{SUPABASE_URL.replace('.co', '.co/storage/v1/object/public')}/videos/{video_id}.mp4"

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
                    "video_file": public_url,
                    "video_download_url": data.get("url", ""),
                    "resolution": f"{data.get('width', 0)}x{data.get('height', 0)}" if data.get("width") else None,
                    "duration": data.get("duration", 0),
                    "status": "uploaded"
                }

                logger.info(f"Inserting video {video_id} into Supabase...")
                supabase.table("videos").upsert(row).execute()

                # Clean up local files
                os.remove(filepath)
                os.remove(local_path)
                logger.info(f"Cleaned up local files for video {video_id}")

            except Exception as e:
                logger.error(f"Failed to process {filename}: {e}")

# === Run script ===
if __name__ == "__main__":
    test_url = "https://www.tiktok.com/@hankgreen1/video/7485820228492954911"
    scrape_single_video(test_url)