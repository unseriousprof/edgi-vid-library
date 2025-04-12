# File: recover_orphaned_videos.py
# Purpose: Fetches metadata for orphaned files in the Supabase videos bucket and adds them to the videos table.

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

# Step 1: Get all tiktok_ids and creator_usernames from the videos table with pagination
tiktok_ids = set()
creator_mapping = {}  # Maps tiktok_id to creator_username for inference
page_size = 1000
offset = 0
while True:
    try:
        response = (
            supabase.table("videos")
            .select("tiktok_id, creator_username")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch_data = response.data
        for item in batch_data:
            tiktok_id = item["tiktok_id"]
            creator_username = item["creator_username"]
            tiktok_ids.add(tiktok_id)
            if creator_username:
                creator_mapping[tiktok_id] = creator_username
        print(f"Fetched {len(batch_data)} tiktok_ids (total so far: {len(tiktok_ids)}).")
        if len(batch_data) < page_size:
            break  # No more rows to fetch
        offset += page_size
    except Exception as e:
        print(f"Error fetching tiktok_ids from videos table: {e}")
        exit(1)

print(f"Found {len(tiktok_ids)} tiktok_ids in the videos table.")

# Step 2: Get all files in the videos bucket with pagination
storage_files = []
path = ""  # Root of the bucket
limit = 1000
offset = 0
while True:
    try:
        response = supabase.storage.from_("videos").list(path=path, options={"limit": limit, "offset": offset})
        if not response:
            print("Storage list response is empty.")
            break
        print(f"Fetched {len(response)} files from Storage (offset: {offset}).")
        storage_files.extend(response)
        if len(response) < limit:
            break  # No more files to fetch
        offset += limit
    except Exception as e:
        print(f"Error listing files in videos bucket: {e}")
        print("This may be due to permissions. Ensure your SUPABASE_KEY is the Service Role Key and Storage policies allow listing.")
        exit(1)

print(f"Found {len(storage_files)} files in the videos bucket.")

# Step 3: Identify orphaned files (files in Storage without a matching tiktok_id)
orphaned_files = []
for file in storage_files:
    file_name = file["name"]
    # Extract tiktok_id from file name (remove .mp4 extension)
    tiktok_id = file_name.replace(".mp4", "")
    if tiktok_id not in tiktok_ids:
        orphaned_files.append((tiktok_id, file_name))

print(f"Found {len(orphaned_files)} orphaned files to process.")

# Step 4: Fetch metadata and add rows to videos table
@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
def fetch_video_metadata(video_url):
    ydl_opts = {"quiet": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

success_count = 0
failed_files = []
for tiktok_id, file_name in orphaned_files:
    try:
        # Step 4.1: Infer creator_username
        creator_username = None
        for existing_tiktok_id, username in creator_mapping.items():
            # Simple heuristic: if the tiktok_id is close (same prefix), assume same creator
            if tiktok_id[:5] == existing_tiktok_id[:5]:
                creator_username = username
                break
        if not creator_username:
            creator_username = "unknown"
            print(f"Could not infer creator for {tiktok_id}. Setting to 'unknown'.")

        # Step 4.2: Fetch metadata from TikTok
        video_url = f"https://www.tiktok.com/@{creator_username}/video/{tiktok_id}"
        info = fetch_video_metadata(video_url)
        if not info or 'webpage_url' not in info:
            raise Exception("Incomplete metadata")

        # Step 4.3: Construct video_file URL
        video_file_url = f"https://dqqsldnguadcbqibnmhi.supabase.co/storage/v1/object/public/videos/{file_name}?"

        # Step 4.4: Insert new row into videos table
        video_data = {
            "tiktok_id": tiktok_id,
            "creator_username": creator_username,
            "video_url": info.get("webpage_url"),
            "title": info.get("title"),
            "description": info.get("description"),
            "upload_date": datetime.strptime(info.get("upload_date", "19700101"), "%Y%m%d").date().isoformat(),
            "views": info.get("view_count", 0),
            "likes": info.get("like_count", 0),
            "comments": info.get("comment_count", 0),
            "shares": info.get("share_count", 0),
            "saved": info.get("favorite_count", 0),
            "video_file": video_file_url,
            "resolution": f"{info.get('width', 0)}x{info.get('height', 0)}" if info.get("width") else None,
            "duration": info.get("duration", 0),
            "upload_status": "done",
            "transcribe_status": "pending",
            "tag_status": "pending",
            "failure_count": 0,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }
        supabase.table("videos").insert(video_data).execute()
        print(f"Successfully added {tiktok_id} to videos table (creator: {creator_username}).")
        success_count += 1
    except Exception as e:
        print(f"Failed to process {tiktok_id}: {e}")
        failed_files.append((tiktok_id, file_name, str(e)))

# Step 5: Summary
print("\n=== Recovery Summary ===")
print(f"Total orphaned files processed: {len(orphaned_files)}")
print(f"Successfully added to videos table: {success_count}")
print(f"Failed: {len(failed_files)}")
if failed_files:
    print("Failed files:")
    for tiktok_id, file_name, error in failed_files:
        print(f"  {tiktok_id} ({file_name}): {error}")

# Step 6: Verify row counts with pagination
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

print(f"Total rows in videos table after script: {videos_count}")
print("Script completed.")