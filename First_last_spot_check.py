import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spot_check_usernames")

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Fetch the first 100 videos from April 11, 2025 ===
def fetch_first_100_videos():
    start_date = "2025-04-11T00:00:00+00:00"
    end_date = "2025-04-12T00:00:00+00:00"
    response = (
        supabase.table("videos")
        .select("id, tiktok_id, creator_username, video_url")
        .gte("created_at", start_date)
        .lt("created_at", end_date)
        .order("created_at", desc=False)  # Ascending order for earliest videos
        .limit(100)
        .execute()
    )
    return response.data

# === Fetch the last 100 videos from April 11, 2025 ===
def fetch_last_100_videos():
    start_date = "2025-04-11T00:00:00+00:00"
    end_date = "2025-04-12T00:00:00+00:00"
    response = (
        supabase.table("videos")
        .select("id, tiktok_id, creator_username, video_url")
        .gte("created_at", start_date)
        .lt("created_at", end_date)
        .order("created_at", desc=True)  # Descending order for latest videos
        .limit(100)
        .execute()
    )
    return response.data

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

# === Spot-check a set of videos ===
def spot_check_set(videos, set_name):
    total_checked = 0
    total_issues = 0
    
    logger.info(f"Spot-checking {set_name} ({len(videos)} videos)...")
    
    for video in videos:
        tiktok_id = video["tiktok_id"]
        stored_creator = video["creator_username"]
        correct_creator = get_correct_creator(tiktok_id)
        
        if correct_creator is None:
            logger.warning(f"Skipping tiktok_id {tiktok_id} - couldn't fetch correct creator")
            continue
        
        total_checked += 1
        if stored_creator != correct_creator:
            total_issues += 1
            logger.info(f"Issue found in {set_name}: tiktok_id {tiktok_id}, stored creator: {stored_creator}, correct creator: {correct_creator}")
    
    if total_checked > 0:
        issue_percentage = (total_issues / total_checked) * 100
        logger.info(f"\n=== {set_name} Summary ===")
        logger.info(f"Total videos checked: {total_checked}")
        logger.info(f"Videos with issues: {total_issues}")
        logger.info(f"Percentage with issues: {issue_percentage:.2f}%")
    else:
        logger.info(f"No videos could be checked in {set_name} - all creator fetches failed.")
    
    return total_checked, total_issues

# === Spot-check first and last 100 videos ===
def spot_check_videos():
    first_100 = fetch_first_100_videos()
    last_100 = fetch_last_100_videos()
    
    # Check the first 100 videos
    first_checked, first_issues = spot_check_set(first_100, "First 100 Videos")
    
    # Check the last 100 videos
    last_checked, last_issues = spot_check_set(last_100, "Last 100 Videos")

if __name__ == "__main__":
    spot_check_videos()