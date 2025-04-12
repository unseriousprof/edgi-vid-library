import os
import time
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from multiprocessing import Pool
from tenacity import retry, wait_exponential, stop_after_attempt

# === Config ===
TRANSCRIPTION_MODEL = "AssemblyAI Nano"
BATCH_SIZE = 10  # Reduced to manage AssemblyAI/Supabase load
SLEEP_INTERVAL = 5  # Seconds between batches
MAX_RETRIES = 3
MAX_RUNTIME_HOURS = 6  # Stop after 6 hours
LOG_FILE = "transcription_log.txt"

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler()  # Still print to console
    ]
)
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

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def validate_url(url: str) -> bool:
    try:
        res = requests.head(url, timeout=10)
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        raise

def group_words_into_segments(words: list) -> list:
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
def transcribe_with_assemblyai(video: dict, poll_interval: int = 2) -> dict:
    video_id = video.get("id", "unknown")
    audio_url = video.get("video_file", "").rstrip("?")
    if not audio_url or not isinstance(audio_url, str):
        raise ValueError(f"Invalid or missing video_file for {video_id}")
    if not validate_url(audio_url):
        raise ValueError(f"Video URL not accessible: {audio_url}")
    start_time = time.time()
    payload = {"audio_url": audio_url, "speech_model": "nano"}
    res = requests.post("https://api.assemblyai.com/v2/transcript", json=payload, headers=HEADERS, timeout=10)
    if res.status_code != 200:
        raise Exception(f"AssemblyAI submission failed: {res.text}")
    transcript_id = res.json()["id"]
    while True:
        polling = requests.get(f"https://api.assemblyai.com/v2/transcript/{transcript_id}", headers=HEADERS, timeout=10).json()
        status = polling["status"]
        if status == "completed":
            words = polling.get("words", [])
            segments = group_words_into_segments(words)
            transcription_time = time.time() - start_time
            return {
                "text": polling["text"],
                "segments": segments,
                "word_timestamps": words,
                "language": polling.get("language_code", "en"),
                "duration": int(polling.get("audio_duration", 0)),
                "transcription_time": transcription_time
            }
        elif status == "error":
            raise Exception(f"Transcription error: {polling.get('error', 'Unknown error')}")
        time.sleep(poll_interval)

def process_video(video):
    video_id = video.get("id", "unknown")
    try:
        result = transcribe_with_assemblyai(video)
        return video_id, True, None, result, video
    except Exception as e:
        current_failure_count = supabase.table("videos").select("failure_count").eq("id", video_id).execute().data[0]["failure_count"]
        supabase.table("videos").update({
            "transcribe_status": "error",
            "failure_count": current_failure_count + 1,
            "processing_errors": {"transcription": str(e)}
        }).eq("id", video_id).execute()
        return video_id, False, str(e), None, video

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def update_video(video_update):
    supabase.table("videos").update({
        "transcribe_status": video_update["transcribe_status"],
        "transcribed_at": video_update["transcribed_at"],
        "language": video_update["language"],
        "duration": video_update["duration"],
        "transcription_model_used": video_update["transcription_model_used"],
        "transcription_time": video_update["transcription_time"]
    }).eq("id", video_update["id"]).execute()

def transcribe_videos_continuously():
    start_time = time.time()
    max_runtime_seconds = MAX_RUNTIME_HOURS * 3600
    total_processed = 0
    total_successes = 0
    total_failures = 0

    logger.info("Starting continuous transcription process...")

    while time.time() - start_time < max_runtime_seconds:
        batch_start_time = time.time()
        # Fetch batch of pending videos
        response = (
            supabase.table("videos")
            .select("*")
            .eq("upload_status", "done")
            .eq("transcribe_status", "pending")
            .limit(BATCH_SIZE)
            .execute()
        )
        videos = response.data
        if not videos:
            logger.info("No videos to transcribe, sleeping for 30 seconds...")
            time.sleep(30)
            continue

        # Mark videos as processing
        video_ids = [v["id"] for v in videos]
        supabase.table("videos").update({"transcribe_status": "processing"}).in_("id", video_ids).execute()

        batch_size = len(videos)
        logger.info(f"Processing batch of {batch_size} videos")
        successes = []
        failures = []

        with Pool(processes=BATCH_SIZE) as pool:
            results = pool.map(process_video, videos)

        transcript_data = []
        video_updates = []
        for video_id, success, error, result, video in results:
            total_processed += 1
            if success:
                transcript_data.append({
                    "video_id": video_id,
                    "transcript": result["text"],
                    "transcript_segments": result["segments"],
                    "word_timestamps": result["word_timestamps"]
                })
                video_updates.append({
                    "id": video_id,
                    "transcribe_status": "done",
                    "transcribed_at": datetime.now(timezone.utc).isoformat(),
                    "language": result["language"],
                    "duration": result["duration"],
                    "transcription_model_used": TRANSCRIPTION_MODEL,
                    "transcription_time": result["transcription_time"]
                })
                successes.append(video_id)
                logger.info(f"[{total_processed}] Transcribed video {video_id} in {result['transcription_time']:.1f}s")
            else:
                failures.append((video_id, error))
                total_failures += 1
                logger.error(f"[{total_processed}] Failed video {video_id}: {error}")

        # Update Supabase
        if transcript_data:
            supabase.table("transcripts").insert(transcript_data).execute()
        if video_updates:
            for update in video_updates:
                update_video(update)

        # Retry failed videos
        if failures:
            logger.info("Retrying failed videos in batch...")
            for video_id, _ in failures:
                video = next(v for v in videos if v["id"] == video_id)
                supabase.table("videos").update({
                    "transcribe_status": "pending",
                    "failure_count": 0,
                    "processing_errors": None
                }).eq("id", video_id).execute()
                try:
                    result = process_video(video)
                    total_processed += 1
                    if result[1]:
                        successes.append(video_id)
                        total_successes += 1
                        logger.info(f"[Retry] Transcribed video {video_id} in {result[3]['transcription_time']:.1f}s")
                        transcript_data = [{
                            "video_id": video_id,
                            "transcript": result[3]["text"],
                            "transcript_segments": result[3]["segments"],
                            "word_timestamps": result[3]["word_timestamps"]
                        }]
                        supabase.table("transcripts").insert(transcript_data).execute()
                        video_update = {
                            "id": video_id,
                            "transcribe_status": "done",
                            "transcribed_at": datetime.now(timezone.utc).isoformat(),
                            "language": result[3]["language"],
                            "duration": result[3]["duration"],
                            "transcription_model_used": TRANSCRIPTION_MODEL,
                            "transcription_time": result[3]["transcription_time"]
                        }
                        update_video(video_update)
                    else:
                        total_failures += 1
                        logger.error(f"[Retry] Failed video {video_id} again: {result[2]}")
                except Exception as e:
                    total_failures += 1
                    logger.error(f"[Retry] Failed video {video_id} again: {e}")

        batch_time = time.time() - batch_start_time
        logger.info(f"Batch completed: {len(successes)} successes, {len(failures)} failures in {batch_time:.1f}s")
        time.sleep(SLEEP_INTERVAL)

    # Final summary
    total_time = time.time() - start_time
    logger.info("\n=== Transcription Summary ===")
    logger.info(f"Total videos processed: {total_processed}")
    logger.info(f"Successfully transcribed: {total_successes}")
    logger.info(f"Failed: {total_failures}")
    logger.info(f"Total runtime: {total_time:.1f} seconds ({total_time/3600:.2f} hours)")

if __name__ == "__main__":
    transcribe_videos_continuously()