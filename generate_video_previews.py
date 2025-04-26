import os
import requests
from supabase import create_client, Client
from moviepy import VideoFileClip
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from multiprocessing import Pool
import time

# === Config ===
BATCH_SIZE = 20
TEST_BATCH_SIZE = 5  # For test run
SLEEP_INTERVAL = 2  # Seconds between batches to avoid Supabase rate limits
MAX_RUNTIME_HOURS = 40  # Stop after 12 hours
LOG_FILE = "video_preview_failures.log"

# === Logging Setup ===
# Only log to console by default, with minimal output
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger()

# Disable verbose HTTP request logging from Supabase client
logging.getLogger("httpx").setLevel(logging.WARNING)

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    logger.error("Missing environment variables")
    exit(1)

# === Supabase setup ===
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def process_video(args):
    video, idx, total = args
    video_id = video["id"]
    video_url = video["video_file"]

    # Temporary paths for video and preview
    video_path = f"temp_videos/{video_id}.mp4"
    preview_path = f"temp_previews/{video_id}.jpg"

    try:
        # Download the video file
        response = requests.get(video_url, timeout=30)
        response.raise_for_status()
        with open(video_path, "wb") as f:
            f.write(response.content)

        # Generate a preview image at 1 second
        clip = VideoFileClip(video_path)
        frame_time = 1.0  # Get frame at 1 second
        clip.save_frame(preview_path, t=frame_time)
        clip.close()

        # Upload the preview image to the assets/previews folder with correct content type
        preview_storage_path = f"previews/{video_id}.jpg"
        with open(preview_path, "rb") as f:
            supabase.storage.from_("assets").upload(
                preview_storage_path,
                f,
                file_options={"content-type": "image/jpeg"}
            )

        # Generate the public URL for the preview image
        preview_url = f"{SUPABASE_URL}/storage/v1/object/public/assets/{preview_storage_path}"

        # Update the videos table with the preview_url
        supabase.table("videos").update({"preview_url": preview_url}).eq("id", video_id).execute()

        # Clean up temporary files
        os.remove(video_path)
        os.remove(preview_path)

        return video_id, True, None
    except Exception as e:
        # Clean up any temporary files if they exist
        if os.path.exists(video_path):
            os.remove(video_path)
        if os.path.exists(preview_path):
            os.remove(preview_path)
        return video_id, False, str(e)

def generate_previews(test_mode=False, failed_videos=None):
    start_time = time.time()
    max_runtime_seconds = MAX_RUNTIME_HOURS * 3600
    total_processed = 0
    total_successes = 0
    total_failures = 0
    failed_videos_list = []

    # Create temporary directories
    if not os.path.exists("temp_videos"):
        os.makedirs("temp_videos")
    if not os.path.exists("temp_previews"):
        os.makedirs("temp_previews")

    if test_mode:
        logger.info("Starting test run...")
    else:
        logger.info("Starting video preview generation...")

    batch_size = TEST_BATCH_SIZE if test_mode else BATCH_SIZE
    max_batches = 1 if test_mode else float('inf')
    batch_count = 0

    while time.time() - start_time < max_runtime_seconds and batch_count < max_batches:
        batch_start_time = time.time()

        # Fetch batch of videos that need previews (no preview_url yet)
        if failed_videos:
            # Rerun mode: process only failed videos
            response = (
                supabase.table("videos")
                .select("id, video_file")
                .in_("id", failed_videos)
                .limit(batch_size)
                .execute()
            )
        else:
            # Normal mode: process videos without preview_url
            response = (
                supabase.table("videos")
                .select("id, video_file")
                .is_("preview_url", "null")
                .limit(batch_size)
                .execute()
            )
        videos = response.data
        if not videos:
            logger.info("No videos to process, exiting...")
            break

        batch_size_actual = len(videos)
        logger.info(f"Processing batch of {batch_size_actual} videos...")
        successes = []
        failures = []

        batch_with_idx = [(video, i + 1, batch_size_actual) for i, video in enumerate(videos)]
        with Pool(processes=batch_size_actual) as pool:
            results = pool.map(process_video, batch_with_idx)

        for video_id, success, error in results:
            total_processed += 1
            if success:
                successes.append(video_id)
                total_successes += 1
            else:
                failures.append((video_id, error))
                failed_videos_list.append((video_id, error))
                total_failures += 1

        batch_time = time.time() - batch_start_time
        logger.info(f"Batch completed: {len(successes)} successes, {len(failures)} failures in {batch_time:.1f} seconds")
        batch_count += 1
        if not test_mode:
            time.sleep(SLEEP_INTERVAL)

    # Clean up directories
    if os.path.exists("temp_videos"):
        for file in os.listdir("temp_videos"):
            os.remove(os.path.join("temp_videos", file))
        os.rmdir("temp_videos")
    if os.path.exists("temp_previews"):
        for file in os.listdir("temp_previews"):
            os.remove(os.path.join("temp_previews", file))
        os.rmdir("temp_previews")

    # Comprehensive final summary
    total_time = time.time() - start_time
    logger.info("\n=== Video Preview Generation Summary ===")
    logger.info(f"Total videos processed: {total_processed}")
    logger.info(f"Successfully processed: {total_successes}")
    logger.info(f"Failed: {total_failures}")
    logger.info(f"Total runtime: {total_time:.1f} seconds ({total_time/3600:.2f} hours)")

    # If there are failures, create a log file with details
    if failed_videos_list:
        # Set up file logging for failures
        file_handler = logging.FileHandler(LOG_FILE, mode="w")
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(file_handler)

        logger.info("\nFailed Videos (to rerun, pass these IDs to the script):")
        failed_ids = [video_id for video_id, _ in failed_videos_list]
        logger.info(f"Failed Video IDs: {failed_ids}")
        for video_id, error in failed_videos_list:
            logger.info(f"  Video ID: {video_id}, Error: {error}")

        # Remove the file handler to stop logging to file
        logger.removeHandler(file_handler)
        file_handler.close()
    else:
        logger.info("No failures encountered.")

if __name__ == "__main__":
    # Test run with a small batch (already done, so comment out)
    # logger.info("Starting test run...")
    # generate_previews(test_mode=True)
    # logger.info("Test run completed. Check results before running on full dataset.")

    # Run on the full dataset
    generate_previews(test_mode=False)

    # To rerun failed videos, you can manually pass the failed IDs like this:
    # failed_ids = ["id1", "id2", ...]  # Copy from log
    # generate_previews(test_mode=False, failed_videos=failed_ids)