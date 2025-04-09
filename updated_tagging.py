import os
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from multiprocessing import Pool
from tenacity import retry, wait_exponential, stop_after_attempt
import google.generativeai as genai

# === Config ===
MODEL_NAME = "gemini-2.0-flash-lite"
BATCH_SIZE = 2  # Small batch size for testing
MAX_RETRIES = 3  # Number of retries per video
SLEEP_INTERVAL = 5  # Seconds to pause between batches
USE_STRUCTURED_OUTPUT = True
TEST_LIMIT = 5  # Test on 5 videos

# === Load environment secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
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
        },
        "onboarding_categories": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["category", "confidence"]
            }
        },
        "difficulty_level": {
            "type": "object",
            "properties": {
                "level": {"type": "string"},
                "confidence": {"type": "number"}
            },
            "required": ["level", "confidence"]
        },
        "engagement_metrics": {
            "type": "object",
            "properties": {
                "attention_grabbing": {"type": "number"},
                "educational_value": {"type": "number"},
                "entertainment_value": {"type": "number"}
            },
            "required": ["attention_grabbing", "educational_value", "entertainment_value"]
        },
        "content_flags": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "flag": {"type": "string"},
                    "confidence": {"type": "number"}
                },
                "required": ["flag", "confidence"]
            }
        }
    },
    "required": ["categories", "topics", "onboarding_categories", "difficulty_level", "engagement_metrics", "content_flags"]
}

# === Prompt Builder ===
def build_prompt(transcript):
    return f"""You are a world-class educational video analyst.

Extract metadata from this TikTok video transcript in the following steps:

**Step 1: Categories**
Identify specific fields that the video could fall under. Such as:
   - "Astrophysics"
   - "Ancient History"
   - "Organic Chemistry"
   - "Zoology"
   - "Paleontology"
   - "Evolution"
   - "European History"
   - etc.
Include a confidence score (0.0-1.0) for each. Return multiple categories if applicable. Only include those clearly supported by the transcript.

**Step 2: Topics**
Identify specific concepts, events, or entities covered in the video, such as:
   - "black holes"
   - "supply and demand"
   - "Plato's Republic"
   - "chemical bond"
   - etc.
Include a confidence score (0.0-1.0) for each. Return multiple topics if applicable. Only include those clearly supported by the transcript.

**Step 3: Onboarding Categories**
Map the video to the following predefined categories that will be used for user onboarding. Always select at least one category. If a video's categories don't align with the predefined list (e.g., a video about neuroscience), use "Other". Select only those that apply, and include a confidence score (0.0-1.0) for each.
   Predefined categories:
   - Geography
   - Space
   - Physics
   - Chemistry
   - Technology
   - News & Politics
   - Psychology
   - History
   - Life Sciences
   - Economics
   - Engineering & How Things Work
   - Math & Logic
   - Other
   - Fun Facts

**Step 4: Difficulty Level**
Determine the difficulty level of the video based on the complexity of the language, concepts, and required background knowledge.

Return a JSON object with the following structure:
{{
  "level": "beginner" or "intermediate" or "advanced",
  "confidence": 0.0 to 1.0
}}

Use these definitions:
- "beginner": Simple language, basic concepts, no prior knowledge needed.
- "intermediate": Moderate complexity, some technical terms, requires basic background knowledge.
- "advanced": Complex concepts, technical language, requires significant prior knowledge.

**Step 5: Engagement Metrics**
Predict the following engagement metrics based on the transcript. Provide scores (0.0-1.0) for each.
   Consider tone, storytelling, clarity, emotional appeal, and any signals of humor or performance:
   - "attention_grabbing": How likely is the video to capture a viewer's immediate attention?
   - "educational_value": How much could someone learn from this (e.g., depth of explanation, clarity of concepts)?
   - "entertainment_value": How entertaining is the video (e.g., humor, storytelling, engaging delivery)?

**Step 6: Content Flags**
Identify potentially sensitive content in the video and assign flags from the following list. Include a confidence score (0.0-1.0) for each flag. If no flags apply, return an empty array.
   - "graphic_violence": Description of violence & gore.
   - "political_content": Political topics that might be controversial.
   - "profanity": Use of inappropriate language.
   - "misinformation_risk": Claims that might be unverified or misleading.

**Edge Cases**:
- Non-educational (e.g., general blogging, jokes, opinion): Return "categories": [{{"tag": "not_educational", "confidence": X}}], "topics": [{{"topic": "not_educational", "confidence": X}}], "onboarding_categories": [{{"category": "not_educational", "confidence": X}}], "difficulty_level": {{}}, "engagement_metrics": {{}}, "content_flags": []
- Too short or vague (may be educational, but can't tell from transcript — e.g., might be an educational animation with no voice over): Return "categories": [{{"tag": "insufficient_transcript", "confidence": 1.0}}], "topics": [{{"topic": "insufficient_transcript", "confidence": 1.0}}], "onboarding_categories": [{{"category": "insufficient_transcript", "confidence": 1.0}}], "difficulty_level": {{}}, "engagement_metrics": {{}}, "content_flags": []

Transcript:
\"\"\"
{transcript}
\"\"\"
"""

@retry(wait=wait_exponential(multiplier=1, min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def tag_transcript(transcript: str) -> dict:
    """Tag a transcript using Gemini with structured output."""
    prompt = build_prompt(transcript)
    start_time = time.time()

    result = model.generate_content(
        prompt,
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": response_schema
        } if USE_STRUCTURED_OUTPUT else None
    )
    parsed_result = json.loads(result.text)

    # Validate and clamp confidence scores
    for category in parsed_result.get("categories", []):
        conf = category.get("confidence", 0)
        category["confidence"] = max(0, min(1, conf))

    for topic in parsed_result.get("topics", []):
        conf = topic.get("confidence", 0)
        topic["confidence"] = max(0, min(1, conf))

    for onboarding_category in parsed_result.get("onboarding_categories", []):
        conf = onboarding_category.get("confidence", 0)
        onboarding_category["confidence"] = max(0, min(1, conf))

    difficulty = parsed_result.get("difficulty_level", {})
    conf = difficulty.get("confidence", 0)
    difficulty["confidence"] = max(0, min(1, conf))

    engagement = parsed_result.get("engagement_metrics", {})
    for key, value in engagement.items():
        engagement[key] = max(0, min(1, value))

    for flag in parsed_result.get("content_flags", []):
        conf = flag.get("confidence", 0)
        flag["confidence"] = max(0, min(1, conf))

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
                "onboarding_categories": [{"category": "insufficient_transcript", "confidence": 1.0}],
                "difficulty_level": {},
                "predictive_engagement": {},
                "content_flags": [],
                "tag_status": "done",
                "tagged_at": datetime.now(timezone.utc).isoformat(),
                "tagging_model_used": MODEL_NAME,
                "processing_errors": "Insufficient transcript",
                "tagging_time": 0.0
            }
        else:
            tags_and_metrics = tag_transcript(transcript)
            update = {
                "categories": tags_and_metrics["categories"],
                "topics": tags_and_metrics["topics"],
                "onboarding_categories": tags_and_metrics["onboarding_categories"],
                "difficulty_level": tags_and_metrics["difficulty_level"],
                "predictive_engagement": tags_and_metrics["engagement_metrics"],
                "content_flags": tags_and_metrics["content_flags"],
                "tag_status": "done",
                "tagged_at": datetime.now(timezone.utc).isoformat(),
                "tagging_model_used": MODEL_NAME,
                "processing_errors": None,
                "tagging_time": tags_and_metrics["tagging_time"]
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

def tag_videos_in_batch(limit: int = TEST_LIMIT):
    """Tag all transcribed videos in batches."""
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

    if not videos:
        print("No videos to tag.")
        return

    successes = []
    failures = []

    # Process in batches with parallel processing
    total_batches = (total_videos + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, total_videos, BATCH_SIZE):
        batch = videos[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} of {total_batches}...")

        with Pool(processes=BATCH_SIZE) as pool:
            results = pool.map(process_video, batch)

        for video_id, success, error in results:
            if success:
                successes.append(video_id)
            else:
                failures.append((video_id, error))

        time.sleep(SLEEP_INTERVAL)  # Pause to avoid rate limiting

    # Summary
    print("\n=== Tagging Summary ===")
    print(f"Total videos processed: {total_videos}")
    print(f"Successfully tagged: {len(successes)}")
    if successes:
        print(f"Success IDs: {', '.join(successes[:5])}{'...' if len(successes) > 5 else ''}")
    print(f"Failed: {len(failures)}")
    if failures:
        print("Failures:")
        for vid, err in failures[:5]:
            print(f"  {vid}: {err}")
        if len(failures) > 5:
            print(f"  ...and {len(failures) - 5} more")

if __name__ == "__main__":
    tag_videos_in_batch(limit=TEST_LIMIT)