import os
import json
import logging
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from multiprocessing import Pool
from tenacity import retry, wait_exponential, stop_after_attempt
import google.generativeai as genai

# === Config ===
MODEL_NAME = "gemini-2.0-flash-lite"
BATCH_SIZE = 3  # Start small for testing
MAX_RETRIES = 3  # Number of retries per video
SLEEP_INTERVAL = 5  # Seconds to pause between batches
USE_STRUCTURED_OUTPUT = True

# === Setup Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tag_transcripts")

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    logger.error("Missing required environment variables")
    exit(1)

# === Supabase and Gemini setup ===
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(f"models/{MODEL_NAME}")

# === Structured Output Schema ===
response_schema = {
    "type": "object",
    "properties": {
        "categories": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "tag": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["tag", "confidence"]
            }
        },
        "topics": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "topic": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["topic", "confidence"]
            }
        }
    },
    "required": ["categories", "topics"]
}

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def tag_transcript(transcript: str) -> dict:
    """Tag a transcript using Gemini with structured output."""
    prompt = f"""
You are a world-class educational video analyst.

Extract metadata from this TikTok video transcript:

1. **categories**: Broad academic subjects or fields, such as, but not limited to:
   - "science"
   - "physics"
   - "chemistry"
   - "biology"
   - "economics"
   - "history"
   - "philosophy"
   - "technology"

2. **topics**: Specific concepts, events, or entities, such as:
   - "photosynthesis"
   - "supply and demand"
   - "World War II"
   - "Plato's Republic"
   - "Great Depression"

Include a confidence score (0.0-1.0) for each. Return multiple categories/topics if applicable, as many as relevant.

Edge Cases:
- Non-educational (e.g. general blogging, jokes, opinion): Return "categories": [{{"tag": "not_educational", "confidence": X}}], "topics": [{{"topic": "not_educational", "confidence": X}}]
- Too short or vague (may be educational, but can’t tell from transcript — e.g. might be an educational animation with no voice over): Return "categories": [{{"tag": "insufficient_transcript", "confidence": 1.0}}], "topics": [{{"topic": "insufficient_transcript", "confidence": 1.0}}]

Transcript:
\"\"\"
{transcript}
\"\"\"
"""
    # Start timing the tagging process
    start_time = time.time()

    result = model.generate_content(
        prompt,
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": response_schema
        } if USE_STRUCTURED_OUTPUT else None
    )
    parsed_result = json.loads(result.text)

    # Validate confidence scores
    for category in parsed_result.get("categories", []):
        conf = category.get("confidence", 0)
        if not (0 <= conf <= 1):
            logger.warning(f"Invalid confidence for tag '{category.get('tag')}': {conf}")

    for topic in parsed_result.get("topics", []):
        conf = topic.get("confidence", 0)
        if not (0 <= conf <= 1):
            logger.warning(f"Invalid confidence for topic '{topic.get('topic')}': {conf}")

    # Calculate tagging time
    tagging_time = time.time() - start_time
    parsed_result["tagging_time"] = tagging_time
    return parsed_result

def process_video(video):
    """Process a single video for tagging."""
    video_id = video["id"]
    try:
        # Fetch transcript from transcripts table
        transcript_data = supabase.table("transcripts").select("transcript").eq("video_id", video_id).execute().data
        if not transcript_data:
            raise ValueError(f"No transcript found for video {video_id}")

        transcript = transcript_data[0]["transcript"].strip()
        if not transcript or len(transcript) < 20:
            update = {
                "categories": [{"tag": "insufficient_transcript", "confidence": 1.0}],
                "topics": [{"topic": "insufficient_transcript", "confidence": 1.0}],
                "tag_status": "done",
                "tagged_at": datetime.now(timezone.utc).isoformat(),
                "tagging_model_used": MODEL_NAME,
                "processing_errors": None,
                "tagging_time": 0.0
            }
        else:
            tags_and_topics = tag_transcript(transcript)
            update = {
                "categories": tags_and_topics["categories"],
                "topics": tags_and_topics["topics"],
                "tag_status": "done",
                "tagged_at": datetime.now(timezone.utc).isoformat(),
                "tagging_model_used": MODEL_NAME,
                "processing_errors": None,
                "tagging_time": tags_and_topics["tagging_time"]
            }

        supabase.table("videos").update(update).eq("id", video_id).execute()
        return video_id, True, None

    except Exception as e:
        current_failure_count = supabase.table("videos").select("failure_count").eq("id", video_id).execute().data[0]["failure_count"]
        supabase.table("videos").update({
            "tag_status": "error",
            "failure_count": current_failure_count + 1,
            "processing_errors": {"tagging": str(e)},
            "tagging_model_used": MODEL_NAME,
            "tagging_time": None
        }).eq("id", video_id).execute()
        return video_id, False, str(e)

def tag_videos_in_batch(limit: int = 100):
    """Tag all transcribed videos in batches."""
    logger.info("Fetching transcribed videos from Supabase...")
    response = (
        supabase.table("videos")
        .select("id")
        .eq("transcribe_status", "done")
        .eq("tag_status", "pending")
        .limit(limit)
        .execute()
    )
    videos = response.data
    total_videos = len(videos)
    logger.info(f"Found {total_videos} videos to tag")

    if not videos:
        logger.info("No videos to tag.")
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

        time.sleep(SLEEP_INTERVAL)  # Pause to avoid rate limiting

    # Summary
    logger.info("\n=== Tagging Summary ===")
    logger.info(f"Total videos processed: {total_videos}")
    logger.info(f"Successfully tagged: {len(successes)}")
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
    tag_videos_in_batch(limit=10)  # Start with a small limit for testing