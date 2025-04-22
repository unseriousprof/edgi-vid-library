# File: overnight_tagging.py
# Purpose: Tags videos with freeform categories and educational rating, storing results in transcripts table.

import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from multiprocessing import Pool
from tenacity import retry, wait_exponential, stop_after_attempt
import google.generativeai as genai
import logging

# === Config ===
MODEL_NAME = "gemini-2.0-flash-lite"
BATCH_SIZE = 12
TEST_BATCH_SIZE = 5  # For test run
MAX_RETRIES = 3
SLEEP_INTERVAL = 5  # Seconds between batches to avoid Supabase rate limits
MAX_RUNTIME_HOURS = 6  # Stop after 6 hours
LOG_FILE = "overnight_tagging_log.txt"

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler()  # Still print to console
    ]
)
logger = logging.getLogger()

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    logger.error("Missing environment variables")
    exit(1)

# === Supabase and Gemini setup ===
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(f"models/{MODEL_NAME}")

# === Structured Output Schema ===
response_schema = {
    "type": "object",
    "properties": {
        "all_categories": {"type": "array", "items": {"type": "string"}},
        "educational_rating": {
            "type": "object",
            "properties": {"educational_rating": {"type": "number", "nullable": True}},
            "required": ["educational_rating"]
        }
    },
    "required": ["all_categories", "educational_rating"]
}

# === Prompt Builder ===
def build_prompt(transcript, description):
    return f"""You are a world-class educational video analyst.

Analyze the transcript and description of this TikTok video to extract metadata. Focus on the transcript as the primary source of information. If the transcript is empty or very short, use the description to infer content, but only if it clearly indicates educational value.

**Step 1: Freeform Categories**
Identify the subject-level categories that best describe the video's content. These should be broad, academic, or thematic fields that reflect the video's focus. Assign as many categories as are relevant, ranging from general to specific, based solely on the transcript and description. Generate these organically.

Return the categories as a JSON array of strings. For example:
[
  "Mathematics",
  "Geometry",
  "Trigonometry"
]

**Step 2: Educational Rating**
Determine how educational the video is on a scale from 0.0 to 1.0, where:
   - 0.0: Not educational at all (e.g., purely trivial, no factual content, vlogs or jokes).
   - 0.2-0.4: Minimally educational (e.g., a small nod to a topic, concept, or idea, but no meaningful depth).
   - 0.5-0.7: Moderately educational (e.g., teaches at least one concept with reasonable depth).
   - 0.8-1.0: Highly educational (e.g., teaches at least one concept with significant depth or covers multiple complex topics thoroughly).
Consider the following factors:
   - Presence of factual content, explanations, or teachings.
   - Depth and clarity of the educational material.
   - Number of concepts and topics covered in the video.
Be discerning and use the full range based on the video's content. Return the rating as a JSON object:
{{
  "educational_rating": 0.25
}}

**Edge Cases**:
- Non-educational (e.g., vlogs, jokes, opinion pieces): Return "all_categories": ["not_educational"], "educational_rating": {{"educational_rating": 0.0}}
- Transcript too short or vague with no useful description, making it hard to discern educational value: Return "all_categories": ["insufficient_data"], "educational_rating": {{"educational_rating": null}}

Transcript:
{transcript}
Description:
{description}
"""

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def tag_transcript(transcript: str, description: str) -> dict:
    prompt = build_prompt(transcript, description)
    start_time = time.time()
    try:
        result = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json", "response_schema": response_schema}
        )
        parsed_result = json.loads(result.text)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Gemini API response: {str(e)}")
        raise ValueError(f"Invalid JSON response from Gemini API: {str(e)}")
    tagging_time = time.time() - start_time
    parsed_result["tagging_time"] = tagging_time
    return parsed_result

def process_transcript(args):
    transcript, idx, total, max_attempts = args
    video_id = transcript["video_id"]
    attempt = 0
    while attempt < max_attempts:
        try:
            transcript_text = transcript.get("transcript", "").strip()
            description = transcript.get("description", "").strip()
            
            if not transcript_text or len(transcript_text) < 20:
                if description and len(description) > 10:
                    tags_and_metrics = tag_transcript("", description)
                else:
                    update = {
                        "all_categories": ["insufficient_data"],
                        "educational_rating": {"educational_rating": None},
                        "tagging_time": 0.0
                    }
                    supabase.table("transcripts").update(update).eq("video_id", video_id).execute()
                    return video_id, True, None, 0.0
            else:
                tags_and_metrics = tag_transcript(transcript_text, description)

            update = {
                "all_categories": tags_and_metrics["all_categories"],
                "educational_rating": tags_and_metrics["educational_rating"],
                "tagging_time": tags_and_metrics["tagging_time"]
            }
            supabase.table("transcripts").update(update).eq("video_id", video_id).execute()
            return video_id, True, None, tags_and_metrics["tagging_time"]

        except Exception as e:
            attempt += 1
            if attempt == max_attempts:
                # Log the error and mark as failed after max attempts
                supabase.table("transcripts").update({
                    "processing_errors": {"tagging": str(e)},
                    "tagging_time": None
                }).eq("video_id", video_id).execute()
                return video_id, False, str(e), 0.0
            logger.warning(f"Attempt {attempt} failed for video {video_id}: {str(e)}. Retrying...")
            time.sleep(2)  # Short delay before retry

def tag_transcripts_continuously(test_mode=False):
    start_time = time.time()
    max_runtime_seconds = MAX_RUNTIME_HOURS * 3600
    total_processed = 0
    total_successes = 0
    total_failures = 0
    failed_videos = []
    total_tagging_time = 0.0

    logger.info("Starting freeform tagging process...")

    batch_size = TEST_BATCH_SIZE if test_mode else BATCH_SIZE
    max_batches = 1 if test_mode else float('inf')
    batch_count = 0

    while time.time() - start_time < max_runtime_seconds and batch_count < max_batches:
        batch_start_time = time.time()
        # Fetch batch of transcripts that need tagging (no all_categories yet)
        response = (
            supabase.table("transcripts")
            .select("video_id, transcript, description")
            .is_("all_categories", "null")
            .limit(batch_size)
            .execute()
        )
        transcripts = response.data
        if not transcripts:
            logger.info("No transcripts to tag, exiting...")
            break

        batch_size_actual = len(transcripts)
        logger.info(f"Processing batch of {batch_size_actual} transcripts")
        successes = []
        failures = []

        batch_with_idx = [(transcript, i + 1, batch_size_actual, MAX_RETRIES) for i, transcript in enumerate(transcripts)]
        with Pool(processes=batch_size_actual) as pool:
            results = pool.map(process_transcript, batch_with_idx)
        
        for video_id, success, error, tagging_time in results:
            total_processed += 1
            if success:
                successes.append(video_id)
                total_successes += 1
                total_tagging_time += tagging_time
                logger.info(f"[{total_processed}] Tagged transcript for video {video_id} in {tagging_time:.1f}s")
            else:
                failures.append((video_id, error))
                failed_videos.append((video_id, error))
                total_failures += 1
                logger.error(f"[{total_processed}] Failed transcript for video {video_id}: {error}")

        batch_time = time.time() - batch_start_time
        logger.info(f"Batch completed: {len(successes)} successes, {len(failures)} failures in {batch_time:.1f}s")
        batch_count += 1
        if not test_mode:
            time.sleep(SLEEP_INTERVAL)

    # Comprehensive final summary
    total_time = time.time() - start_time
    avg_tagging_time = total_tagging_time / total_successes if total_successes > 0 else 0.0
    logger.info("\n=== Freeform Tagging Summary ===")
    logger.info(f"Total transcripts processed: {total_processed}")
    logger.info(f"Successfully tagged: {total_successes}")
    logger.info(f"Failed: {total_failures}")
    logger.info(f"Average tagging time per successful transcript: {avg_tagging_time:.1f} seconds")
    logger.info(f"Total runtime: {total_time:.1f} seconds ({total_time/3600:.2f} hours)")
    if failed_videos:
        logger.info("\nFailed Videos:")
        for video_id, error in failed_videos:
            logger.info(f"  Video ID: {video_id}, Error: {error}")
    else:
        logger.info("No failures encountered.")

if __name__ == "__main__":
    # Test run with a small batch
    logger.info("Starting test run...")
    tag_transcripts_continuously(test_mode=True)
    logger.info("Test run completed. Check results before running on full dataset.")
    # Uncomment the line below to run on the full dataset overnight
    tag_transcripts_continuously(test_mode=False)