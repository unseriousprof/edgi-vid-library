# File: add_transcript_description.py
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Missing SUPABASE_URL or SUPABASE_KEY in .env file.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_video_metadata(video_url):
    ydl_opts = {"quiet": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

video_id = "7134702660107947310"
username = "minuteearth"

print(f"Adding description for video {video_id} to transcripts...")

# Check if transcript already exists
existing_transcript = supabase.table("transcripts").select("video_id").eq("video_id", (
    supabase.table("videos").select("id").eq("tiktok_id", video_id).single()
)).execute().data
if existing_transcript:
    print(f"Transcript already exists for video {video_id}. Skipping.")
    exit(0)

# Fetch video metadata
video_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
try:
    info = fetch_video_metadata(video_url)
    if not info or 'webpage_url' not in info:
        raise Exception("Incomplete metadata")
    
    # Get video_id from videos table
    video_row = supabase.table("videos").select("id").eq("tiktok_id", video_id).single().execute().data
    video_db_id = video_row["id"]

    # Insert description into transcripts table
    transcript_data = {
        "video_id": video_db_id,
        "description": info.get("description"),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    supabase.table("transcripts").insert(transcript_data).execute()
    print(f"Successfully added description for video {video_id} to transcripts table.")
except Exception as e:
    print(f"Failed to process video {video_id}: {e}")