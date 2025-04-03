import os
import time
import logging
import requests
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
TRANSCRIPTION_MODEL = "whisper-1"

def validate_url(url: str) -> bool:
    """Check if the video URL is accessible."""
    try:
        res = requests.head(url, timeout=5)
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        return False

def transcribe_with_openai(video: dict) -> dict:
    """Submit video to OpenAI Whisper-1 and process into segments."""
    video_id = video.get("id", "unknown")
    audio_url = video.get("video_file", "").rstrip("?")

    if not audio_url or not isinstance(audio_url, str):
        raise ValueError(f"Invalid or missing video_file for {video_id}")
    if not validate_url(audio_url):
        raise ValueError(f"Video URL not accessible: {audio_url}")

    # Start timing the transcription process
    start_time = time.time()

    # Download the audio file temporarily
    audio_response = requests.get(audio_url, timeout=10)
    if audio_response.status_code != 200:
        raise Exception(f"Failed to download audio: {audio_response.status_code}")
    
    temp_audio_path = f"temp_{video_id}.mp3"
    with open(temp_audio_path, "wb") as f:
        f.write(audio_response.content)

    # Transcribe using OpenAI API with segment-level timestamps
    try:
        with open(temp_audio_path, "rb") as audio_file:
            transcription = openai_client.audio.transcriptions.create(
                model="whisper-1",
                file=audio_file,
                response_format="text",
                timestamp_granularities=["segment"]
            )
        
        # Calculate transcription time
        transcription_time = time.time() - start_time

        # Format segments to match original structure
        segments = [{"start": s.start, "end": s.end, "text": s.text} for s in transcription.segments]

        # Clean up temporary file
        os.remove(temp_audio_path)

        return {
            "text": transcription.text,
            "segments": segments,
            "language": transcription.language,
            "duration": int(transcription.duration or 0),
            "transcription_time": transcription_time
        }
    except Exception as e:
        # Clean up temporary file if it exists
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
                "transcript_segments": result["segments"],
                "language": result["language"],
                "duration": result["duration"],
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
    run_transcription_batch(limit=3)