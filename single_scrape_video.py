# File: scrape_single_tiktok_video.py
# Purpose: Scrapes metadata for a single TikTok video and adds to videos and transcripts tables.

import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("single_tiktok_scraper")
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Service Role key for RLS

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Missing SUPABASE_URL or SUPABASE_KEY in .env file.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Functions ===
def fetch_video_metadata(video_url):
    ydl_opts = {"quiet": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

def scrape_single_video(video_id, username):
    print(f"Scraping metadata for video {video_id}...")

    # Check if video already exists in videos table
    existing_video = supabase.table("videos").select("tiktok_id").eq("tiktok_id", video_id).execute().data
    if existing_video:
        print(f"Video {video_id} already exists in videos table. Skipping.")
        return

    # Fetch metadata
    video_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
    try:
        info = fetch_video_metadata(video_url)
        if not info or 'webpage_url' not in info:
            raise Exception("Incomplete metadata")

        # Construct video_data for videos table
        video_data = {
            "tiktok_id": video_id,
            "creator_username": username,
            "video_url": info.get("webpage_url"),
            "title": info.get("title"),
            "upload_date": datetime.strptime(info.get("upload_date", "19700101"), "%Y%m%d").date().isoformat(),
            "views": info.get("view_count", 0),
            "likes": info.get("like_count", 0),
            "comments": info.get("comment_count", 0),
            "shares": info.get("share_count", 0),
            "saved": info.get("favorite_count", 0),
            "resolution": f"{info.get('width', 0)}x{info.get('height', 0)}" if info.get("width") else None,
            "duration": info.get("duration", 0),
            "video_file": f"https://dqqsldnguadcbqibnmhi.supabase.co/storage/v1/object/public/videos/{video_id}.mp4?",
            "upload_status": "done",
            "transcribe_status": "pending",
            "tag_status": "pending",
            "failure_count": 0,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        # Insert into videos table
        video_response = supabase.table("videos").insert(video_data).execute()
        video_row = video_response.data[0]
        video_db_id = video_row["id"]

        # Insert description into transcripts table
        transcript_data = {
            "video_id": video_db_id,
            "description": info.get("description"),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        supabase.table("transcripts").insert(transcript_data).execute()

        print(f"Successfully added video {video_id} to videos and transcripts tables.")
    except Exception as e:
        logger.error(f"Failed to process video {video_id}: {e}")
        # Insert error row into videos table
        video_data = {
            "tiktok_id": video_id,
            "creator_username": username,
            "upload_status": "error",
            "failure_count": 1,
            "processing_errors": {"metadata": str(e)}
        }
        supabase.table("videos").insert(video_data).execute()
        print(f"Added error row for video {video_id} to videos table.")

if __name__ == "__main__":
    video_id = "7134702660107947310"
    username = "minuteearth"  # Confirmed as minuteearth video
    scrape_single_video(video_id, username)