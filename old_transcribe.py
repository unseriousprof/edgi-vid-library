import os
import time
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from multiprocessing import Pool
from tenacity import retry, wait_exponential, stop_after_attempt

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
BATCH_SIZE = 5  # Start small for testing
MAX_RETRIES = 3  # Number of retries per video

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

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def transcribe_with_assemblyai(video: dict, poll_interval: int = 1) -> dict:
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
            transcription_time = time.time() - start_time
            return {
                "text": polling["text"],
                "segments": segments,
                "word_timestamps": words,  # Store word-level timestamps
                "language": polling.get("language_code", "en"),
                "duration": int(polling.get("audio_duration", 0)),
                "transcription_time": transcription_time
            }
        elif status == "error":
            raise Exception(f"Transcription error: {polling.get('error', 'Unknown error')}")
        time.sleep(poll_interval)

def process_video(video):
    """Process a single video for transcription."""
    video_id = video.get("id", "unknown")
    try:
        result = transcribe_with_assemblyai(video)
        
        # Insert transcription data into transcripts table
        transcript_data = {
            "video_id": video_id,
            "transcript": result["text"],
            "transcript_segments": result["segments"],
            "word_timestamps": result["word_timestamps"]
        }
        supabase.table("transcripts").insert(transcript_data).execute()

        # Update videos table on success
        supabase.table("videos").update({
            "transcribe_status": "done",
            "transcribed_at": datetime.now(timezone.utc).isoformat(),
            "language": result["language"],
            "duration": result["duration"],
            "transcription_model_used": TRANSCRIPTION_MODEL,
            "transcription_time": result["transcription_time"]
        }).eq("id", video_id).execute()

        return video_id, True, None

    except Exception as e:
        # Update videos table on failure
        current_failure_count = supabase.table("videos").select("failure_count").eq("id", video_id).execute().data[0]["failure_count"]
        supabase.table("videos").update({
            "transcribe_status": "error",
            "failure_count": current_failure_count + 1,
            "processing_errors": {"transcription": str(e)}
        }).eq("id", video_id).execute()

        return video_id, False, str(e)

def run_transcription_batch(limit: int = 100):
    """Process a batch of videos and report results."""
    logger.info(f"Fetching up to {limit} videos with upload_status='done' and transcribe_status='pending'")
    videos = (
        supabase
        .table("videos")
        .select("*")
        .eq("upload_status", "done")
        .eq("transcribe_status", "pending")
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

    # Process in batches with parallel processing
    for i in range(0, total_videos, BATCH_SIZE):
        batch = videos[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i // BATCH_SIZE + 1} of {(total_videos + BATCH_SIZE - 1) // BATCH_SIZE}")

        with Pool(processes=BATCH_SIZE) as pool:
            results = pool.map(process_video, batch)

        for video_id, success, error in results:
            if success:
                successes.append(video_id)
            else:
                failures.append((video_id, error))

        time.sleep(5)  # Pause to avoid rate limiting

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
    run_transcription_batch(limit=200)  # Start with a small limit for testing