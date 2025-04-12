import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp
import time
from tenacity import retry, wait_exponential, stop_after_attempt
from concurrent.futures import ThreadPoolExecutor, as_completed

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
MAX_WORKERS = 10
SLEEP_INTERVAL = 1
MAX_CREATOR_WORKERS = 3

# === Functions ===
@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def download_video(video_url, output_path):
    ydl_opts = {"outtmpl": output_path, "format": "best"}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

def fetch_video_metadata(video_url):
    ydl_opts = {"quiet": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

def process_video(args):
    video_id, info, username = args
    output_path = os.path.join(VIDEO_FOLDER, f"{video_id}.mp4")
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
            pass
        return video_id, False, str(e)

def pre_check_channel(username):
    """Test if the channel's playlist is accessible before scraping."""
    ydl_opts = {"quiet": True, "extract_flat": True, "playlist_items": "1"}  # Fetch only the first video
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
        return True, None
    except Exception as e:
        return False, str(e)

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(2))
def scrape_channel(username):
    start_time = time.time()
    print(f"Starting scrape for @{username}...")

    # Pre-check the channel
    can_scrape, error = pre_check_channel(username)
    if not can_scrape:
        raise Exception(f"Cannot scrape @{username}: {error}")

    # Fetch the playlist
    ydl_opts = {"quiet": True, "extract_flat": True, "playlistend": None}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            playlist = ydl.extract_info(f"https://www.tiktok.com/@{username}", download=False)
            if not playlist or 'entries' not in playlist:
                raise Exception("Failed to fetch playlist entries")
            video_ids = [entry['id'] for entry in playlist['entries'] if 'id' in entry]
    except Exception as e:
        raise Exception(f"Failed to fetch playlist for @{username}: {str(e)}")

    # Batch the video_ids to avoid Supabase query limits
    CHUNK_SIZE = 500  # Safe limit for Supabase IN clause
    existing_ids = set()
    for i in range(0, len(video_ids), CHUNK_SIZE):
        chunk = video_ids[i:i + CHUNK_SIZE]
        existing_videos = supabase.table("videos").select("tiktok_id").in_("tiktok_id", chunk).execute().data
        existing_ids.update(video['tiktok_id'] for video in existing_videos)

    videos_to_process = [vid for vid in video_ids if vid not in existing_ids]
    total_videos = len(videos_to_process)
    print(f"Found {total_videos} new videos to process")

    successes = []
    failures = []

    for i in range(0, total_videos, BATCH_SIZE):
        batch_ids = videos_to_process[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} of {(total_videos + BATCH_SIZE - 1) // BATCH_SIZE}")
        batch_metadata = []

        # Fetch metadata and insert rows only if successful
        for video_id in batch_ids:
            video_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
            try:
                info = fetch_video_metadata(video_url)
                if not info or 'webpage_url' not in info:
                    raise Exception("Incomplete metadata")
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
                    "resolution": f"{info.get('width', 0)}x{info.get('height', 0)}" if info.get("width") else None,
                    "duration": info.get("duration", 0),
                    "upload_status": "pending",
                    "transcribe_status": "pending",
                    "tag_status": "pending",
                    "failure_count": 0,
                }
                supabase.table("videos").insert(video_data).execute()
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

        # Process videos that have metadata
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = executor.map(process_video, [(video_id, info, username) for video_id, info in batch_metadata])
        
        for idx, (video_id, success, error) in enumerate(results, start=i + 1):
            if success:
                successes.append(video_id)
                print(f"[{idx}/{total_videos}] Uploaded video {video_id}")
            else:
                failures.append(video_id)
                print(f"[{idx}/{total_videos}] Failed video {video_id}")
                # Update the row to error status if download/upload fails
                supabase.table("videos").update({
                    "upload_status": "error",
                    "failure_count": 1,
                    "processing_errors": {"upload": str(error)}
                }).eq("tiktok_id", video_id).execute()

        time.sleep(SLEEP_INTERVAL)

    if failures:
        print("\nRetrying failed videos...")
        retry_successes = []
        retry_failures = []
        
        for video_id in failures:
            supabase.table("videos").update({
                "upload_status": "pending",
                "failure_count": 0,
                "processing_errors": None
            }).eq("tiktok_id", video_id).execute()
            
            video_url = f"https://www.tiktok.com/@{username}/video/{video_id}"
            try:
                info = fetch_video_metadata(video_url)
                result = process_video((video_id, info, username))
                if result[1]:
                    retry_successes.append(video_id)
                    print(f"[Retry] Uploaded video {video_id}")
                else:
                    retry_failures.append(video_id)
                    print(f"[Retry] Failed video {video_id} again")
            except Exception as e:
                logger.error(f"Retry metadata fetch failed for {video_id}: {e}")
                retry_failures.append(video_id)
                supabase.table("videos").update({
                    "upload_status": "error",
                    "failure_count": 1,
                    "processing_errors": {"metadata": str(e)}
                }).eq("tiktok_id", video_id).execute()
                print(f"[Retry] Failed video {video_id} again")

        successes.extend(retry_successes)
        failures = retry_failures

    end_time = time.time()
    runtime_seconds = end_time - start_time
    return {
        "username": username,
        "total_videos": total_videos,
        "successes": successes,
        "failures": failures,
        "runtime_seconds": runtime_seconds
    }

def scrape_multiple_creators(creators):
    total_start_time = time.time()
    results = {}
    
    if len(creators) == 1:
        results[creators[0]] = scrape_channel(creators[0])
    else:
        failed_creators = []
        with ThreadPoolExecutor(max_workers=MAX_CREATOR_WORKERS) as executor:
            future_to_creator = {executor.submit(scrape_channel, creator): creator for creator in creators}
            for future in as_completed(future_to_creator):
                creator = future_to_creator[future]
                try:
                    results[creator] = future.result()
                except Exception as e:
                    print(f"Scraping @{creator} failed: {e}")
                    failed_creators.append(creator)

        if failed_creators:
            print("\nRetrying failed creators...")
            for creator in failed_creators:
                try:
                    results[creator] = scrape_channel(creator)
                    print(f"Retry succeeded for @{creator}")
                except Exception as e:
                    print(f"Retry for @{creator} failed again: {e}")
                    results[creator] = {
                        "username": creator,
                        "total_videos": 0,
                        "successes": [],
                        "failures": [],
                        "runtime_seconds": 0,
                        "error": str(e)
                    }

    print("\n=== Scraping Summaries ===")
    for creator, result in results.items():
        print(f"\nSummary for @{creator}:")
        print(f"Total videos attempted: {result['total_videos']}")
        print(f"Successfully uploaded: {len(result['successes'])}")
        print(f"Failed: {len(result['failures'])}")
        print(f"Runtime: {result['runtime_seconds']:.1f} seconds ({result['runtime_seconds']/60:.2f} minutes)")
        if result['failures']:
            print("Failed video IDs: " + ", ".join(result['failures']))
        if "error" in result:
            print(f"Error: {result['error']}")

    total_end_time = time.time()
    total_runtime = total_end_time - total_start_time
    print(f"\n=== Overall Runtime ===\n{total_runtime:.1f} seconds ({total_runtime/60:.2f} minutes)")

if __name__ == "__main__":
    creators = ["minuteearth"]
    scrape_multiple_creators(creators)