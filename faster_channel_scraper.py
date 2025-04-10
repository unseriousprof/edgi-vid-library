import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp
import time
from tenacity import retry, wait_exponential, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("supa_channel_scraper")
logging.getLogger("httpx").setLevel(logging.WARNING)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

VIDEO_FOLDER = os.path.expanduser("~/Documents/video_tmp")
os.makedirs(VIDEO_FOLDER, exist_ok=True)

BATCH_SIZE = 10
MAX_RETRIES = 3
MAX_WORKERS = 5
SLEEP_INTERVAL = 1

# === Functions ===
@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def download_video(video_url, output_path):
    ydl_opts = {
        "outtmpl": output_path,
        "format": "best",
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

def fetch_video_metadata(video_url):
    ydl_opts = {
        "quiet": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

def process_video(args):
    video_id, info, username = args
    output_path = os.path.join(VIDEO_FOLDER, f"{video_id}.mp4")
    # Fresh Supabase client per thread
    thread_supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    try:
        download_video(info['webpage_url'], output_path)
        with open(output_path, "rb") as file:
            thread_supabase.storage.from_("videos").upload(f"{video_id}.mp4", file, {"content-type": "video/mp4"})
        public_url = thread_supabase.storage.from_("videos").get_public_url(f"{video_id}.mp4")
        thread_supabase.table("videos").update({
            "video_file": public_url,
            "upload_status": "done",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }).eq("tiktok_id", video_id).execute()
        try:
            os.remove(output_path)
        except OSError as e:
            logger.warning(f"Failed to remove {output_path}: {e}")
        return video_id, True, None
    except Exception as e:
        logger.error(f"Upload failed for {video_id}: {e}")
        current_failure_count = thread_supabase.table("videos").select("failure_count").eq("tiktok_id", video_id).execute().data[0]["failure_count"]
        thread_supabase.table("videos").update({
            "upload_status": "error",
            "failure_count": current_failure_count + 1,
            "processing_errors": {"upload": str(e)}
        }).eq("tiktok_id", video_id).execute()
        try:
            os.remove(output_path)
        except OSError:
            pass  # Ignore cleanup failure here
        return video_id, False, str(e)

def scrape_channel(username):
    print(f"Starting scrape for @{username}...")
    ydl_opts = {
        "quiet": True,
        "extract_flat": True,
        "playlistend": None,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        playlist = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
        video_ids = [entry['id'] for entry in playlist['entries']]

    existing_videos = supabase.table("videos").select("tiktok_id").in_("tiktok_id", video_ids).execute().data
    existing_ids = {video['tiktok_id'] for video in existing_videos}

    videos_to_process = [vid for vid in video_ids if vid not in existing_ids]
    total_videos = len(videos_to_process)
    print(f"Found {total_videos} new videos to process")

    successes = []
    failures = []

    for i in range(0, total_videos, BATCH_SIZE):
        batch_ids = videos_to_process[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} of {(total_videos + BATCH_SIZE - 1) // BATCH_SIZE}")
        batch_metadata = []

        for video_id in batch_ids:
            video_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
            try:
                info = fetch_video_metadata(video_url)
                batch_metadata.append((video_id, info))
            except Exception as e:
                logger.error(f"Metadata fetch failed for {video_id}: {e}")
                failures.append(video_id)
                video_data = {
                    "tiktok_id": video_id,
                    "creator_username": username,
                    "upload_status": "error",
                    "failure_count": 1,
                    "processing_errors": {"metadata": str(e)}
                }
                supabase.table("videos").insert(video_data).execute()

        for video_id, info in batch_metadata:
            video_data = {
                "tiktok_id": video_id,
                "creator_username": username,
                "video_url": info.get("webpage_url"),
                "title": info.get("title"),
                "description": info.get("description"),
                "upload_date": datetime.strptime(info.get("upload_date", "19700101"), "%Y%m%d").date().isoformat(),
                "views": info.get("view_count", 0),
                "likes": info.get("like_count", 0),
                "comments": info.get("comment_count", 0),
                "shares": info.get("share_count", 0),
                "saved": info.get("favorite_count", 0),
                "video_download_url": info.get("url", ""),
                "resolution": f"{info.get('width', 0)}x{info.get('height', 0)}" if info.get("width") else None,
                "duration": info.get("duration", 0),
                "upload_status": "pending",
                "transcribe_status": "pending",
                "tag_status": "pending",
                "failure_count": 0,
            }
            supabase.table("videos").insert(video_data).execute()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = executor.map(process_video, [(video_id, info, username) for video_id, info in batch_metadata])
        
        for idx, (video_id, success, error) in enumerate(results, start=i + 1):
            if success:
                successes.append(video_id)
                print(f"[{idx}/{total_videos}] Uploaded video {video_id}")
            else:
                failures.append(video_id)
                print(f"[{idx}/{total_videos}] Failed video {video_id}")

        time.sleep(SLEEP_INTERVAL)

    print("\n=== Scraping Summary ===")
    print(f"Total videos attempted: {total_videos}")
    print(f"Successfully uploaded: {len(successes)}")
    print(f"Failed: {len(failures)}")
    if failures:
        print("Failed video IDs: " + ", ".join(failures))

if __name__ == "__main__":
    username = "reason4math"
    scrape_channel(username)