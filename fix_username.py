import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fix_creator_usernames")

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Fetch affected rows with pagination ===
def fetch_affected_videos():
    PAGE_SIZE = 1000  # Supabase default limit per request
    offset = 0
    all_videos = []
    
    while True:
        # Select videos where created_at is between 2025-04-12 00:00:00 and 2025-04-13 00:00:00
        start_date = "2025-04-12T00:00:00+00:00"
        end_date = "2025-04-13T00:00:00+00:00"
        response = (
            supabase.table("videos")
            .select("id, tiktok_id, creator_username, video_url")
            .gte("created_at", start_date)
            .lt("created_at", end_date)
            .range(offset, offset + PAGE_SIZE - 1)  # Paginate with offset and limit
            .execute()
        )
        videos = response.data
        all_videos.extend(videos)
        
        if len(videos) < PAGE_SIZE:  # If we get fewer rows than the page size, we've reached the end
            break
        
        offset += PAGE_SIZE
        logger.info(f"Fetched {len(all_videos)} videos so far...")
    
    return all_videos

# === Fetch correct creator username using yt_dlp ===
def get_correct_creator(tiktok_id):
    url = f"https://www.tiktok.com/@anyuser/video/{tiktok_id}"
    ydl_opts = {"quiet": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            return info.get("uploader", "").lstrip("@")
    except Exception as e:
        logger.error(f"Failed to fetch creator for tiktok_id {tiktok_id}: {e}")
        return None

# === Update videos in smaller batches ===
def update_videos_in_batches(videos, batch_size=100):  # Reduced from 500 to 100
    total_videos = len(videos)
    logger.info(f"Total videos to update: {total_videos}")
    
    for i in range(0, total_videos, batch_size):
        batch = videos[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} of {(total_videos + batch_size - 1) // batch_size}")
        
        updates = []
        for video in batch:
            tiktok_id = video["tiktok_id"]
            video_id = video["id"]
            correct_creator = get_correct_creator(tiktok_id)
            if correct_creator:
                correct_video_url = f"https://www.tiktok.com/@{correct_creator}/video/{tiktok_id}"
                updates.append({
                    "id": video_id,
                    "creator_username": correct_creator,
                    "video_url": correct_video_url
                })
            else:
                logger.warning(f"Skipping update for tiktok_id {tiktok_id} - couldn't fetch correct creator")
        
        if updates:
            for update in updates:
                supabase.table("videos").update({
                    "creator_username": update["creator_username"],
                    "video_url": update["video_url"]
                }).eq("id", update["id"]).execute()
        
        time.sleep(1)  # Avoid rate limiting

if __name__ == "__main__":
    videos = fetch_affected_videos()
    update_videos_in_batches(videos)