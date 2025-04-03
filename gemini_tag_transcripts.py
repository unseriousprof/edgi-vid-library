import os
import json
import logging
import time
from dotenv import load_dotenv
from supabase import create_client
import google.generativeai as genai

# === Config ===
MODEL_NAME = "gemini-2.0-flash-lite"
BATCH_SIZE = 10  # Process 10 videos per batch
SLEEP_INTERVAL = 2  # Seconds to pause between batches
USE_STRUCTURED_OUTPUT = True

# === Setup Logging ===
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("tag_transcripts")
logger.setLevel(logging.INFO)

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    logger.error("Missing required environment variables")
    exit(1)

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

def tag_transcript(transcript: str) -> dict:
    """Tag a transcript using Gemini with structured output."""
    prompt = f"""
You are a world-class educational video analyst.

Extract metadata from this TikTok video transcript:

1. **categories**: Broad academic subjects or fields, such as:
   - "science"
   - "physics"
   - "chemistry"
   - "biology"
   - "economics"
   - "history"
   - "philosophy"

2. **topics**: Specific concepts, events, or entities, such as:
   - "photosynthesis"
   - "supply and demand"
   - "World War II"
   - "Plato's Republic"
   - "Great Depression"

Include a confidence score (0.0-1.0) for each. Return multiple categories/topics if applicable, as many as relevant.

Edge Cases:
- Non-educational (e.g. general blogging, jokes, opinion): Return "categories": [{{"tag": "not_educational", "confidence": X}}], "topics": [{{"topic": "not_educational", "confidence": X}}]
- Too short or vague: Return "categories": [{{"tag": "insufficient_transcript", "confidence": 1.0}}], "topics": [{{"topic": "insufficient_transcript", "confidence": 1.0}}]

Transcript:
\"\"\"
{transcript}
\"\"\"
"""
    # Start timing the tagging process
    start_time = time.time()

    try:
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

    except Exception as e:
        logger.error(f"Tagging failed: {str(e)}")
        raise

def tag_videos_in_batch():
    """Tag all transcribed videos in batches."""
    logger.info("Fetching transcribed videos from Supabase...")
    response = (
        supabase.table("videos")
        .select("id, transcript")
        .eq("status", "transcribed")
        .execute()
    )
    videos = response.data
    total_videos = len(videos)
    logger.info(f"Found {total_videos} videos to tag")

    if not videos:
        logger.info("No videos to tag.")
        return

    successes = 0
    failures = 0

    for i, video in enumerate(videos, 1):
        video_id = video["id"]
        transcript = video.get("transcript", "").strip()
        logger.info(f"Processing {i}/{total_videos}: {video_id}")

        if not transcript or len(transcript) < 20:
            update = {
                "categories": [{"tag": "insufficient_transcript", "confidence": 1.0}],
                "topics": [{"topic": "insufficient_transcript", "confidence": 1.0}],
                "status": "tagged",
                "tagging_model_used": MODEL_NAME,
                "processing_errors": None,
                "tagging_time": 0.0  # No tagging time for insufficient transcripts
            }
        else:
            try:
                tags_and_topics = tag_transcript(transcript)
                update = {
                    "categories": tags_and_topics["categories"],
                    "topics": tags_and_topics["topics"],
                    "status": "tagged",
                    "tagging_model_used": MODEL_NAME,
                    "processing_errors": None,
                    "tagging_time": tags_and_topics["tagging_time"]  # Add tagging time
                }
                successes += 1
            except Exception as e:
                update = {
                    "processing_errors": {"tagging": str(e)},
                    "status": "error_tagging",
                    "tagging_model_used": MODEL_NAME,
                    "tagging_time": None  # No tagging time on failure
                }
                failures += 1

        supabase.table("videos").update(update).eq("id", video_id).execute()

        if i % BATCH_SIZE == 0 and i < total_videos:
            logger.info(f"Completed batch of {BATCH_SIZE}, pausing for {SLEEP_INTERVAL}s...")
            time.sleep(SLEEP_INTERVAL)

    logger.info("\n=== Tagging Summary ===")
    logger.info(f"Total videos processed: {total_videos}")
    logger.info(f"Successfully tagged: {successes}")
    logger.info(f"Failed: {failures}")

if __name__ == "__main__":
    tag_videos_in_batch()