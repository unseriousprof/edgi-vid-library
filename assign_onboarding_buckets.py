# File: assign_onboarding_buckets.py
# Purpose: For every transcript without onboarding_bucket, ask Gemini to
#          choose ALL relevant onboarding buckets and write them back.

import os, json, time, logging
from dotenv import load_dotenv
from supabase import create_client
from tenacity import retry, wait_exponential, stop_after_attempt
import google.generativeai as genai

# ---------- Config ----------
MODEL_NAME    = "gemini-2.0-flash-lite"
BATCH_SIZE    = 12           # keep it gentle on rate-limits
MAX_RETRIES   = 3
SLEEP_SECONDS = 4
LOG_FILE      = "onboarding_bucket_log.txt"

# The 16 bucket IDs we expose to users
BUCKET_IDS = [
    "ancient_civilizations","art_history","astrophysics",
    "biology_life_sciences","economics_money","fun_facts",
    "geography","how_things_work","lab_experiments",
    "math_physics","other","philosophy","psychology",
    "technology_ai","words_languages","world_history"
]

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, mode="a"), logging.StreamHandler()]
)
log = logging.getLogger()

# ---------- Secrets ----------
load_dotenv()
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    log.error("Missing .env vars"); exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(f"models/{MODEL_NAME}")

# ---------- Gemini response schema ----------
response_schema = {
    "type": "object",
    "properties": { "buckets": { "type": "array", "items": { "type": "string" } } },
    "required": ["buckets"]
}

# ---------- Prompt builder ----------
def build_prompt(transcript:str, description:str, freeform:list[str]) -> str:
    bucket_list = "\n".join(f"- {bid}" for bid in BUCKET_IDS)
    return f"""
You are classifying TikTok videos into **on-boarding buckets** for an education
app. A video can belong to MULTIPLE buckets.

Allowed bucket IDs:
{bucket_list}

Guidelines:
- Pick EVERY bucket that genuinely fits (not just the single best).
- If none fit, return ["other"].
- Use the *content itself* (transcript + description). You may consult the
  free-form topic list for extra clues.

Return ONLY valid JSON matching:
{{ "buckets": ["bucket_id", ...] }}

Transcript:
\"\"\"{transcript}\"\"\"

Description:
\"\"\"{description}\"\"\"

Free-form topics for this video:
{freeform}
"""

@retry(wait=wait_exponential(min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def classify(transcript:str, description:str, topics:list[str]) -> list[str]:
    prompt = build_prompt(transcript, description, topics)
    resp = model.generate_content(
        prompt,
        generation_config = {
            "response_mime_type": "application/json",
            "response_schema": response_schema
        }
    )
    try:
        data = json.loads(resp.text)
        chosen = [b for b in data["buckets"] if b in BUCKET_IDS]
        return chosen if chosen else ["other"]
    except json.JSONDecodeError as e:
        raise ValueError(f"Bad JSON from Gemini: {e}")

# ---------- Main worker ----------
def run(test_mode=False):
    batch_limit = 30 if test_mode else BATCH_SIZE
    while True:
        res = (
            supabase.table("transcripts")
            .select("video_id, transcript, description, all_categories")
            .is_("onboarding_bucket", "null")
            .limit(batch_limit)
            .execute()
        )
        rows = res.data
        if not rows:
            log.info("🎉  All transcripts tagged. Done.")
            break

        log.info(f"Processing {len(rows)} transcripts…")
        for row in rows:
            vid   = row["video_id"]
            trans = (row.get("transcript")   or "").strip()
            desc  = (row.get("description")  or "").strip()
            cats  = row.get("all_categories") or []

            try:
                buckets = classify(trans, desc, cats)
                supabase.table("transcripts").update(
                    {"onboarding_bucket": buckets}
                ).eq("video_id", vid).execute()
                log.info(f"{vid} → {buckets}")
            except Exception as e:
                log.error(f"Failed on {vid}: {e}")

        if test_mode:
            log.info("Test batch complete — stopping because test_mode=True")
            break
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    # log.info("🔍  Smoke-test run")
    # run(test_mode=True)

    # When happy, comment out the smoke-test line above and uncomment below:
    log.info("🚀  Full dataset run")
    run(test_mode=False)