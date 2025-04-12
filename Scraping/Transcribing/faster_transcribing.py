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

TRANSCRIPTION_MODEL = "AssemblyAI Nano"
BATCH_SIZE = 30 
SLEEP_INTERVAL = 1
MAX_RETRIES = 3

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
def validate_url(url: str) -> bool:
    try:
        res = requests.head(url, timeout=10)  # Increased from 5s to 10s
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"URL validation failed for {url}: {e}")
        raise  # Let tenacity retry

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

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(3))
def update_video(video_update):
    supabase.table("videos").update({
        "transcribe_status": video_update["transcribe_status"],
        "transcribed_at": video_update["transcribed_at"],
        "language": video_update["language"],
        "duration": video_update["duration"],
        "transcription_model_used": video_update["transcription_model_used"],
        "transcription_time": video_update["transcription_time"]
    }).eq("id", video_update["id"]).execute()

def run_transcription_batch(limit: int = 100):
    start_time = time.time()
    print(f"Fetching up to {limit} videos with upload_status='done' and transcribe_status='pending'")
    videos = (
        supabase.table("videos")
        .select("*")
        .eq("upload_status", "done")
        .eq("transcribe_status", "pending")
        .limit(limit)
        .execute()
        .data
    )

    total_videos = len(videos)
    print(f"Found {total_videos} video(s) to process")
    if total_videos == 0:
        print("No videos to process. Exiting.")
        return

    successes = []
    failures = []

    for i in range(0, total_videos, BATCH_SIZE):
        batch = videos[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} of {(total_videos + BATCH_SIZE - 1) // BATCH_SIZE}")
        with Pool(processes=BATCH_SIZE) as pool:
            results = pool.map(process_video, batch)

        transcript_data = []
        video_updates = []
        for video_id, success, error, result, video in results:
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
                print(f"[{len(successes) + len(failures)}/{total_videos}] Transcribed video {video_id} in {result['transcription_time']:.1f}s")
            else:
                failures.append((video_id, error))
                print(f"[{len(successes) + len(failures)}/{total_videos}] Failed video {video_id}: {error}")

        if transcript_data:
            supabase.table("transcripts").insert(transcript_data).execute()
        if video_updates:
            for update in video_updates:
                update_video(update)

        time.sleep(SLEEP_INTERVAL)

    if failures:
        print("\nRetrying failed videos...")
        retry_successes = []
        retry_failures = []
        for video_id, _ in failures:
            video = next(v for v in videos if v["id"] == video_id)
            supabase.table("videos").update({
                "transcribe_status": "pending",
                "failure_count": 0,
                "processing_errors": None
            }).eq("id", video_id).execute()
            try:
                result = process_video(video)
                if result[1]:
                    retry_successes.append(video_id)
                    print(f"[Retry] Transcribed video {video_id} in {result[3]['transcription_time']:.1f}s")
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
                    update_video(video_update)  # Use the retry-wrapped function
                else:
                    retry_failures.append((video_id, result[2]))
                    print(f"[Retry] Failed video {video_id} again: {result[2]}")
            except Exception as e:
                retry_failures.append((video_id, str(e)))
                print(f"[Retry] Failed video {video_id} again: {e}")
        successes.extend(retry_successes)
        failures = retry_failures

    end_time = time.time()
    runtime_seconds = end_time - start_time
    print("\n=== Transcription Summary ===")
    print(f"Total videos attempted: {total_videos}")
    print(f"Successfully transcribed: {len(successes)}")
    print(f"Failed: {len(failures)}")
    print(f"Total runtime: {runtime_seconds:.1f} seconds ({runtime_seconds/60:.2f} minutes)")
    if failures:
        print("Failed video IDs: " + ", ".join([vid for vid, _ in failures]))

if __name__ == "__main__":
    run_transcription_batch(limit=1000)