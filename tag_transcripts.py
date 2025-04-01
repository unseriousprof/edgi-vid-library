import os
import json
import logging
import google.generativeai as genai
from dotenv import load_dotenv
from supabase import create_client

# === Config ===
MODEL_NAME = "gemini-2.0-flash-lite"

# === Setup ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tag_transcripts")

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Connect to Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Set up Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(f"models/{MODEL_NAME}")

# === Main logic ===
def tag_transcripts():
    logger.info("Fetching transcribed videos from Supabase...")

    response = (
        supabase.table("videos")
        .select("*")
        .eq("status", "transcribed")
        .limit(1)  # Remove this limit to process all transcribed videos
        .execute()
    )

    for video in response.data:
        video_id = video["id"]
        transcript = video.get("transcript", "")

        if not transcript or len(transcript.strip()) < 20:
            logger.warning(f"Video {video_id} has too little transcript, skipping.")
            update = {
                "tags": [{"tag": "insufficient_transcript", "confidence": 1.0}],
                "topics": [{"topic": "insufficient_transcript", "confidence": 1.0}],
                "tagging_model_used": MODEL_NAME,
                "status": "tagged"
            }
            supabase.table("videos").update(update).eq("id", video_id).execute()
            continue

        logger.info(f"Tagging video {video_id}...")

        prompt = f"""
You are a world-class educational video analyst.

Your job is to extract useful **metadata** from the transcript of an educational TikTok video.

Please return:

1. **tags** = broad academic subject areas, like:
   - "science"
   - "physics"
   - "chemistry"
   - "economics"
   - "history"
   - "philosophy"

2. **topics** = specific concepts, events, or ideas covered in the video, like:
   - "inflation"
   - "Bayes' Theorem"
   - "World War II"

Each tag or topic must include a confidence score (between 0.0 and 1.0). The transcript may include multiple tags and multiple topics.

Edge Cases:
- If the transcript is clearly not educational (e.g. dancing, jokes), return just one tag and one topic: `"not_educational"` with confidence 1.0
- If the transcript is too short to tell, return `"insufficient_transcript"` with confidence 1.0

Return **only valid JSON** in the following format:

{{
  "tags": [{{ "tag": "string", "confidence": float }}],
  "topics": [{{ "topic": "string", "confidence": float }}]
}}

Transcript:
\"\"\"
{transcript}
\"\"\"
"""

        try:
            result = model.generate_content(prompt)
            logger.info(f"Gemini raw response:\n{result.text}")

            # Remove backtick markdown if present (```json)
            text = result.text.strip()
            if text.startswith("```"):
                text = text.strip("`")
                if text.lower().startswith("json"):
                    text = text[4:].strip()

            parsed = json.loads(text)

            update = {
                "tags": parsed.get("tags", []),
                "topics": parsed.get("topics", []),
                "tagging_model_used": MODEL_NAME,
                "status": "tagged",
                "processing_errors": None
            }

            supabase.table("videos").update(update).eq("id", video_id).execute()
            logger.info(f"✅ Tagged video {video_id} successfully.")

        except Exception as e:
            logger.error(f"❌ Error tagging video {video_id}: {e}")
            supabase.table("videos").update({
                "processing_errors": {"tagging": str(e)},
                "tagging_model_used": MODEL_NAME
            }).eq("id", video_id).execute()

# === Entry point ===
if __name__ == "__main__":
    tag_transcripts()