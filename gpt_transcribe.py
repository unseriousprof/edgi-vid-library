import os
import time
import logging
import requests
import ffmpeg
from dotenv import load_dotenv
from supabase import create_client
from openai import OpenAI

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("openai_transcribe")

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, OPENAI_API_KEY]):
    logger.error("Missing required environment variables")
    exit(1)

# === Supabase + OpenAI setup ===
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Define the transcription model used
TRANSCRIPTION_MODEL = "gpt-4o-mini-transcribe"

def validate_url(url: str) -> bool:
    """Check if the video URL is accessible."""
    try:
        res = requests.head(url, timeout=5)
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        return False

def extract_audio_from_video(video_path: str, audio_path: str):
    """Extract audio from video file using ffmpeg."""
    try:
        stream = ffmpeg.input(video_path)
        stream = ffmpeg.output(stream, audio_path, format="mp3", acodec="mp3", loglevel="quiet")
        ffmpeg.run(stream)
        logger.info(f"Extracted audio to {audio_path}")
    except ffmpeg.Error as e:
        raise Exception(f"FFmpeg audio extraction failed: {e.stderr.decode()}")

def transcribe_with_openai(video: dict) -> dict:
    """Submit video to OpenAI GPT-4o-mini-transcribe and return plain text transcription."""
    video_id = video.get("id", "unknown")
    video_url = video.get("video_file", "").rstrip("?")

    if not video_url or not isinstance(video_url, str):
        raise ValueError(f"Invalid or missing video_file for {video_id}")
    if not validate_url(video_url):
        raise ValueError(f"Video URL not accessible: {video_url}")

    # Start timing the transcription process
    start_time = time.time()

    # Download the video file temporarily
    logger.info(f"Downloading video from {video_url}")
    video_response = requests.get(video_url, timeout=10)
    if video_response.status_code != 200:
        raise Exception(f"Failed to download video: {video_response.status_code}")
    
    temp_video_path = f"temp_video_{video_id}.mp4"
    temp_audio_path = f"temp_audio_{video_id}.mp3"
    
    with open(temp_video_path, "wb") as f:
        f.write(video_response.content)
    
    # Log video file size
    video_size = os.path.getsize(temp_video_path) / (1024 * 1024)  # Size in MB
    logger.info(f"Downloaded video size: {video_size:.2f} MB")

    # Extract audio
    extract_audio_from_video(temp_video_path, temp_audio_path)

    # Log audio file size
    audio_size = os.path.getsize(temp_audio_path) / (1024 * 1024)  # Size in MB
    logger.info(f"Extracted audio size: {audio_size:.2f} MB")

    # Transcribe using OpenAI API with text response format
    try:
        with open(temp_audio_path, "rb") as audio_file:
            transcription = openai_client.audio.transcriptions.create(
                model="gpt-4o-mini-transcribe",
                file=audio_file,
                response_format="text"
            )
        
        # Calculate transcription time
        transcription_time = time.time() - start_time

        # Clean up temporary files
        os.remove(temp_video_path)
        os.remove(temp_audio_path)

        # Return only text and transcription time since timestamps aren't available
        return {
            "text": transcription,
            "transcription_time": transcription_time
        }
    except Exception as e:
        # Clean up temporary files if they exist
        if os.path.exists(temp_video_path):
            os.remove(temp_video_path)
        if os.path.exists(temp_audio_path):
            os.remove(temp_audio_path)
        raise Exception(f"OpenAI transcription failed: {str(e)}")

def run_transcription_batch(limit: int = 100):
    """Process a batch of videos and report results."""
    logger.info(f"Fetching up to {limit} videos with status='uploaded'")
    videos = (
        supabase
        .table("videos")
        .select("*")
        .eq("status", "uploaded")
        .limit(limit)
        .execute()
        .data
    )

    total_videos = len(videos)
    logger.info(f"Found {total_videos} video(s) to process")
    if total_videos == 0:
        logger.info("No videos to process. Exiting.")
        return

    successes = []
    failures = []

    for i, video in enumerate(videos, 1):
        video_id = video.get("id", "unknown")
        logger.info(f"Processing video {i}/{total_videos}: {video_id}")
        try:
            result = transcribe_with_openai(video)
            update = {
                "transcript": result["text"],
                "transcript_segments": None,  # No segments available
                "language": None,  # Not provided in text format
                "duration": None,  # Not provided in text format
                "status": "transcribed",
                "processing_errors": None,
                "transcription_model_used": TRANSCRIPTION_MODEL,
                "transcription_time": result["transcription_time"]
            }
            supabase.table("videos").update(update).eq("id", video_id).execute()
            successes.append(video_id)
        except Exception as e:
            logger.error(f"Failed video {video_id}: {e}")
            supabase.table("videos").update({
                "status": "error_transcribing",
                "processing_errors": {"transcription": str(e)},
                "transcription_model_used": TRANSCRIPTION_MODEL
            }).eq("id", video_id).execute()
            failures.append((video_id, str(e)))

    # Summary
    logger.info("\n=== Transcription Summary ===")
    logger.info(f"Total videos processed: {total_videos}")
    logger.info(f"Successfully transcribed: {len(successes)}")
    if successes:
        logger.info(f"Success IDs: {', '.join(successes[:5])}{'...' if len(successes) > 5 else ''}")
    logger.info(f"Failed: {len(failures)}")
    if failures:
        logger.info("Failures:")
        for vid, err in failures[:5]:
            logger.info(f"  {vid}: {err}")
        if len(failures) > 5:
            logger.info(f"  ...and {len(failures) - 5} more")

if __name__ == "__main__":
    run_transcription_batch(limit=3)  # Process only 3 videos