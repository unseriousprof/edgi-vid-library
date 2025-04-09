import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
import google.generativeai as genai

# === Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_tagging")

# === Load env vars ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Validate environment variables
if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    logger.error("Missing required environment variables")
    exit(1)

# === Supabase + Gemini Setup ===
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("models/gemini-2.0-flash-lite")

# === Replace with actual video ID ===
VIDEO_ID = "0ccf54d6-cd89-4b9c-b213-6e58e250ed77"

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

# === Tag a Single Video ===
def tag_single_video(video_id):
    # Get transcript from Supabase
    result = supabase.table("transcripts").select("transcript").eq("video_id", video_id).execute()
    if not result.data:
        logger.error(f"No transcript found for video {video_id}")
        return

    transcript = result.data[0]["transcript"].strip()
    if not transcript or len(transcript) < 20:
        logger.warning(f"Transcript for video {video_id} is too short or empty")
        update = {
            "categories": [{"tag": "insufficient_transcript", "confidence": 1.0}],
            "topics": [{"topic": "insufficient_transcript", "confidence": 1.0}],
            "onboarding_categories": [{"category": "insufficient_transcript", "confidence": 1.0}],
            "difficulty_level": {},
            "predictive_engagement": {},
            "content_flags": [],
            "tag_status": "done",
            "tagged_at": datetime.now(timezone.utc).isoformat(),
            "tagging_model_used": "gemini-2.0-flash-lite",
            "processing_errors": "Insufficient transcript",
            "tagging_time": 0.0
        }
    else:
        prompt = build_prompt(transcript)
        try:
            response = model.generate_content(
                prompt,
                generation_config={
                    "response_mime_type": "application/json",
                    "response_schema": response_schema
                }
            )
            # Log the raw response for debugging
            logger.info(f"Raw response from Gemini API: {response.text}")
            parsed = json.loads(response.text)

            # Validate confidence scores
            for category in parsed.get("categories", []):
                conf = category.get("confidence", 0)
                if not (0 <= conf <= 1):
                    logger.warning(f"Invalid confidence for tag '{category.get('tag')}': {conf}")
                    category["confidence"] = max(0, min(1, conf))

            for topic in parsed.get("topics", []):
                conf = topic.get("confidence", 0)
                if not (0 <= conf <= 1):
                    logger.warning(f"Invalid confidence for topic '{topic.get('topic')}': {conf}")
                    topic["confidence"] = max(0, min(1, conf))

            for onboarding_category in parsed.get("onboarding_categories", []):
                conf = onboarding_category.get("confidence", 0)
                if not (0 <= conf <= 1):
                    logger.warning(f"Invalid confidence for onboarding category '{onboarding_category.get('category')}': {conf}")
                    onboarding_category["confidence"] = max(0, min(1, conf))

            difficulty = parsed.get("difficulty_level", {})
            conf = difficulty.get("confidence", 0)
            if not (0 <= conf <= 1):
                logger.warning(f"Invalid confidence for difficulty level: {conf}")
                difficulty["confidence"] = max(0, min(1, conf))

            engagement = parsed.get("engagement_metrics", {})
            for key, value in engagement.items():
                if not (0 <= value <= 1):
                    logger.warning(f"Invalid value for engagement metric '{key}': {value}")
                    engagement[key] = max(0, min(1, value))

            for flag in parsed.get("content_flags", []):
                conf = flag.get("confidence", 0)
                if not (0 <= conf <= 1):
                    logger.warning(f"Invalid confidence for content flag '{flag.get('flag')}': {conf}")
                    flag["confidence"] = max(0, min(1, conf))

            # Build update payload
            update = {
                "categories": parsed.get("categories"),
                "topics": parsed.get("topics"),
                "onboarding_categories": parsed.get("onboarding_categories"),
                "difficulty_level": parsed.get("difficulty_level", {}),
                "predictive_engagement": parsed.get("engagement_metrics", {}),
                "content_flags": parsed.get("content_flags", []),
                "tag_status": "done",
                "tagged_at": datetime.now(timezone.utc).isoformat(),
                "tagging_model_used": "gemini-2.0-flash-lite",
                "processing_errors": None,
                "tagging_time": None  # You might want to add timing logic here
            }
        except Exception as e:
            logger.error(f"Failed to tag video {video_id}: {str(e)}")
            update = {
                "tag_status": "error",
                "processing_errors": str(e),
                "tagging_model_used": "gemini-2.0-flash-lite",
                "tagging_time": None
            }

    # Update the video in Supabase
    supabase.table("videos").update(update).eq("id", video_id).execute()
    logger.info(f"✅ Tagging complete for {video_id}")

# === Run It ===
if __name__ == "__main__":
    tag_single_video("a9e312b6-c1f4-4b4a-9135-67426c1656c8")