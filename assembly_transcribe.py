import os
import time
import logging
import requests
from dotenv import load_dotenv
from supabase import create_client

# === Logging Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("assembly_transcribe")

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, ASSEMBLYAI_API_KEY]):
    logger.error("Missing required environment variables")
    exit(1)

# === Supabase + AssemblyAI setup ===
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
HEADERS = {
    "authorization": ASSEMBLYAI_API_KEY,
    "content-type": "application/json"
}

# Define the transcription model used
TRANSCRIPTION_MODEL = "AssemblyAI"

def validate_url(url: str) -> bool:
    """Check if the video URL is accessible."""
    try:
        res = requests.head(url, timeout=5)
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        return False

def group_words_into_segments(words: list) -> list:
    """Group words into phrase-like segments based on punctuation or time gaps."""
    if not words:
        return []

    segments = []
    current_segment = {"start": words[0]["start"], "text": ""}
    last_end = 0

    for i, word in enumerate(words):
        current_segment["text"] += word["text"] + " "
        last_end = word["end"]

        is_punctuation = word["text"].endswith((".", "!", "?", ",", ";"))
        is_last_word = i == len(words) - 1
        next_word = words[i + 1] if i + 1 < len(words) else None
        has_gap = next_word and (next_word["start"] - word["end"] > 1000)

        if is_punctuation or has_gap or is_last_word:
            current_segment["end"] = last_end
            segments.append(current_segment)
            current_segment = {"start": next_word["start"] if next_word else last_end, "text": ""}

    return [s for s in segments if s["text"].strip()]

def transcribe_with_assemblyai(video: dict, poll_interval: int = 5) -> dict:
    """Submit video to AssemblyAI and process into phrase segments."""
    video_id = video.get("id", "unknown")
    audio_url = video.get("video_file", "").rstrip("?")

    if not audio_url or not isinstance(audio_url, str):
        raise ValueError(f"Invalid or missing video_file for {video_id}")
    if not validate_url(audio_url):
        raise ValueError(f"Video URL not accessible: {audio_url}")

    # Start timing the transcription process
    start_time = time.time()

    payload = {"audio_url": audio_url, "speech_model": "nano"}
    res = requests.post("https://api.assemblyai.com/v2/transcript", json=payload, headers=HEADERS, timeout=10)

    if res.status_code != 200:
        raise Exception(f"AssemblyAI submission failed: {res.text}")

    transcript_id = res.json()["id"]

    while True:
        polling = requests.get(
            f"https://api.assemblyai.com/v2/transcript/{transcript_id}",
            headers=HEADERS,
            timeout=10
        ).json()

        status = polling["status"]
        if status == "completed":
            words = polling.get("words", [])
            segments = group_words_into_segments(words)
            # Calculate transcription time
            transcription_time = time.time() - start_time
            return {
                "text": polling["text"],
                "segments": segments,
                "language": polling.get("language_code", "en"),
                "duration": int(polling.get("audio_duration", 0)),
                "transcription_time": transcription_time
            }
        elif status == "error":
            raise Exception(f"Transcription error: {polling.get('error', 'Unknown error')}")
        time.sleep(poll_interval)

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
            result = transcribe_with_assemblyai(video)
            update = {
                "transcript": result["text"],
                "transcript_segments": result["segments"],
                "language": result["language"],
                "duration": result["duration"],
                "status": "transcribed",
                "processing_errors": None,
                "transcription_model_used": TRANSCRIPTION_MODEL,  # Add transcription model
                "transcription_time": result["transcription_time"]  # Add transcription time
            }
            supabase.table("videos").update(update).eq("id", video_id).execute()
            successes.append(video_id)
        except Exception as e:
            logger.error(f"Failed video {video_id}: {e}")
            supabase.table("videos").update({
                "status": "error_transcribing",
                "processing_errors": {"transcription": str(e)},
                "transcription_model_used": TRANSCRIPTION_MODEL  # Still record model on failure
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
        for vid, err in failures[:5]:  # Limit to 5 for brevity
            logger.info(f"  {vid}: {err}")
        if len(failures) > 5:
            logger.info(f"  ...and {len(failures) - 5} more")

if __name__ == "__main__":
    run_transcription_batch()