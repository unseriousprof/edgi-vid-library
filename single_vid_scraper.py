import os
import sys
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import yt_dlp
from tenacity import retry, wait_exponential, stop_after_attempt

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("single_video_scraper")

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

VIDEO_FOLDER = os.path.expanduser("~/Documents/video_tmp")
os.makedirs(VIDEO_FOLDER, exist_ok=True)

MAX_RETRIES = 3  # Number of retries per video

# === Functions ===
@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def download_video(video_url, output_path):
    """Download a video with retries."""
    ydl_opts = {
        "outtmpl": output_path,
        "format": "best",
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])

def fetch_video_metadata(video_url):
    """Fetch video metadata without downloading."""
    ydl_opts = {
        "quiet": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(video_url, download=False)
    return info

def scrape_single_video(video_url):
    """Scrape and upload a single TikTok video."""
    try:
        # Fetch metadata
        info = fetch_video_metadata(video_url)
        video_id = info.get("id")
        username = info.get("uploader_id")

        # Check if video already exists
        existing = supabase.table("videos").select("tiktok_id").eq("tiktok_id", video_id).execute().data
        if existing:
            logger.info(f"Video {video_id} already exists in the database. Skipping.")
            return video_id, True

        # Prepare video data
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

        # Insert video with pending status
        supabase.table("videos").insert(video_data).execute()

        # Download and upload video
        output_path = os.path.join(VIDEO_FOLDER, f"{video_id}.mp4")
        download_video(video_url, output_path)

        # Upload to Supabase storage
        with open(output_path, "rb") as file:
            supabase.storage  # Ensure content-type is specified
            supabase.storage.from_("videos").upload(f"{video_id}.mp4", file, {"content-type": "video/mp4"})

        # Get public URL
        public_url = supabase.storage.from_("videos").get_public_url(f"{video_id}.mp4")

        # Update database on success
        supabase.table("videos").update({
            "video_file": public_url,
            "upload_status": "done",
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }).eq("tiktok_id", video_id).execute()

        # Cleanup
        os.remove(output_path)

        logger.info(f"Successfully uploaded video {video_id}")
        return video_id, True

    except Exception as e:
        logger.error(f"Failed to process video {video_id}: {e}")

        # Update database on failure
        current_failure_count = supabase.table("videos").select("failure_count").eq("tiktok_id", video_id).execute().data[0]["failure_count"]
        supabase.table("videos").update({
            "upload_status": "error",
            "failure_count": current_failure_count + 1,
            "processing_errors": {"upload": str(e)}
        }).eq("tiktok_id", video_id).execute()

        return video_id, False

# === Main Execution ===
if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python single_video_scraper.py <tiktok_video_url>")
        sys.exit(1)

    video_url = sys.argv[1]
    video_id, success = scrape_single_video(video_url)

    if success:
        logger.info(f"Video {video_id} processed successfully.")
    else:
        logger.error(f"Failed to process video {video_id}. Check logs for details.")