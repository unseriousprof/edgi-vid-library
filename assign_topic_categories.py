# File: assign_topic_categories.py
# Purpose: For each row in public.topics with category=NULL,
#          call Gemini once and set category -> one of our 16 onboarding IDs.

import os, json, time, logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from supabase import create_client
from tenacity import retry, wait_exponential, stop_after_attempt
import google.generativeai as genai

# === Config ===
MODEL_NAME      = "gemini-2.0-flash-lite"
BATCH_SIZE      = 20          # bigger is fine; topics are tiny payloads
MAX_RETRIES     = 3
SLEEP_BETWEEN   = 3           # seconds between batches
LOG_FILE        = "topic_bucket_log.txt"

# --- our 16 bucket IDs (exactly as they appear in categories.id) ---
BUCKET_IDS = [
    "ancient_civilizations", "art_history", "astrophysics",
    "biology_life_sciences", "economics_money", "fun_facts",
    "geography", "how_things_work", "lab_experiments",
    "math_physics", "other", "philosophy", "psychology",
    "technology_ai", "words_languages", "world_history"
]

# === Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, mode="a"), logging.StreamHandler()]
)
log = logging.getLogger()

# === Secrets ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not all([SUPABASE_URL, SUPABASE_KEY, GEMINI_API_KEY]):
    log.error("Missing .env vars");  exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(f"models/{MODEL_NAME}")

# === Gemini response schema ===
response_schema = {
    "type": "object",
    "properties": { "bucket_id": { "type": "string" } },
    "required": ["bucket_id"]
}

# === Prompt builder ===
def build_prompt(topic_name: str) -> str:
    bucket_list = "\n".join(f"- {bid}" for bid in BUCKET_IDS)
    return f"""You are helping categorize educational topics.

Given ONE topic *exactly as written* below, choose the **single best** bucket ID
from the approved list. If no bucket is even remotely appropriate, choose `other`.

Respond ONLY with valid JSON matching this schema:
{{ "bucket_id": "<one_of_the_ids_below>" }}

Approved bucket IDs:
{bucket_list}

Topic to categorize (verbatim):
\"\"\"{topic_name}\"\"\"
"""

@retry(wait=wait_exponential(min=4, max=60), stop=stop_after_attempt(MAX_RETRIES))
def classify_topic(topic_name: str) -> str:
    prompt = build_prompt(topic_name)
    resp = model.generate_content(
        prompt,
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": response_schema
        }
    )
    try:
        data = json.loads(resp.text)
        bucket_id = data["bucket_id"]
        if bucket_id not in BUCKET_IDS:
            raise ValueError(f"Invalid bucket_id {bucket_id}")
        return bucket_id
    except json.JSONDecodeError as e:
        raise ValueError(f"Bad JSON: {e}")

def run(test_mode=False):
    limit = 40 if test_mode else BATCH_SIZE
    while True:
        res = (
            supabase.table("topics")
            .select("id, name")
            .is_("category", "null")
            .limit(limit)
            .execute()
        )
        todo = res.data
        if not todo:
            log.info("🎉  All topics bucketed. Done.")
            break

        log.info(f"Processing {len(todo)} topics…")
        for row in todo:
            topic_id, topic_name = row["id"], row["name"]
            try:
                bucket_id = classify_topic(topic_name)
                supabase.table("topics").update(
                    {"category": bucket_id}
                ).eq("id", topic_id).execute()
                log.info(f"{topic_name} → {bucket_id}")
            except Exception as e:
                log.error(f"Failed for '{topic_name}': {e}")

        if not test_mode:
            time.sleep(SLEEP_BETWEEN)
        else:
            break   # one batch & quit

if __name__ == "__main__":
    # log.info("🔍  Test run starting…")
    # run(test_mode=True)     # smoke test first
    # When happy, comment it out and uncomment below:
    log.info("🚀  Full run starting…")
    run(test_mode=False)