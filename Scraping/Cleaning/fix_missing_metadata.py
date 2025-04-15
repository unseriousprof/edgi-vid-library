# File: fix_missing_metadata.py
# Purpose: Fixes missing metadata in the videos table (views, title, description, resolution, duration, comments, likes).

import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp
from tenacity import retry, wait_exponential, stop_after_attempt

# Load environment variables from .env file
load_dotenv()

# Get Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Validate credentials
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Missing SUPABASE_URL or SUPABASE_KEY in .env file.")
    exit(1)

# Initialize Supabase client
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Successfully connected to Supabase.")
except Exception as e:
    print(f"Error connecting to Supabase: {e}")
    exit(1)

# Step 1: Fetch videos with missing metadata
missing_metadata_videos = []
page_size = 1000
offset = 0
while True:
    try:
        response = (
            supabase.from_("videos")
            .select("videos.id, videos.tiktok_id, videos.video_url, creators(username)")
            .or_("views.eq.0,title.is.null,description.is.null,resolution.is.null,duration.eq.0,comments.is.null,likes.is.null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch_data = response.data
        missing_metadata_videos.extend(batch_data)
        print(f"Fetched {len(batch_data)} videos with missing metadata (total so far: {len(missing_metadata_videos)}).")
        if len(batch_data) < page_size:
            break
        offset += page_size
    except Exception as e:
        print(f"Error fetching videos with missing metadata: {e}")
        exit(1)

print(f"Found {len(missing_metadata_videos)} videos with missing metadata.")

# Step 2: Fetch metadata from TikTok
@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
def fetch_video_metadata(video_url):
    ydl_opts = {"quiet": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

# Step 3: Process videos with missing metadata
success_count = 0
failed_videos = []
for video in missing_metadata_videos:
    try:
        tiktok_id = video["tiktok_id"]
        creator_username = video["creator"]["username"]
        video_url = video["video_url"] if video["video_url"] else f"https://www.tiktok.com/@{creator_username}/video/{tiktok_id}"
        info = fetch_video_metadata(video_url)
        if not info or "webpage_url" not in info:
            raise Exception("Incomplete metadata")
        # Update the video row with missing metadata (excluding saved and shares)
        update_data = {
            "video_url": info.get("webpage_url"),
            "title": info.get("title"),
            "description": info.get("description"),
            "upload_date": datetime.strptime(info.get("upload_date", "19700101"), "%Y%m%d").date().isoformat(),
            "views": info.get("view_count", 0),
            "likes": info.get("like_count", 0),
            "comments": info.get("comment_count", 0),
            "resolution": f"{info.get('width', 0)}x{info.get('height', 0)}" if info.get("width") else None,
            "duration": info.get("duration", 0),
        }
        supabase.table("videos").update(update_data).eq("id", video["id"]).execute()
        print(f"Updated metadata for {tiktok_id}.")
        success_count += 1
    except Exception as e:
        print(f"Failed to update metadata for {tiktok_id}: {e}")
        failed_videos.append((tiktok_id, str(e)))

# Step 4: Summary
print("\n=== Script Summary ===")
print(f"Total videos processed: {len(missing_metadata_videos)}")
print(f"Successfully updated: {success_count}")
print(f"Failed: {len(failed_videos)}")
if failed_videos:
    print("Failed updates:")
    for tiktok_id, error in failed_videos:
        print(f"  {tiktok_id}: {error}")

# Step 5: Verify row counts
videos_count = 0
offset = 0
while True:
    try:
        response = (
            supabase.table("videos")
            .select("id")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        videos_count += len(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size
    except Exception as e:
        print(f"Error fetching final videos count: {e}")
        exit(1)

print(f"\nTotal rows in videos table after script: {videos_count}")
print("Script completed.")
