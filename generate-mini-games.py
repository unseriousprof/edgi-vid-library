# assign_mini_games.py  (Python 3.11)
import os, json, random, time, logging
from dotenv import load_dotenv
from supabase import create_client
from tenacity import retry, wait_exponential, stop_after_attempt
import google.generativeai as genai

# ---------- CONFIG ----------
MODEL_NAME     = "gemini-2.5-flash-preview-04-17"
SAMPLE_SIZE    = 1000          # top 1000 videos by views
MAX_RETRIES    = 3
SLEEP_SECONDS  = 4
LOG_FILE       = "mini_game_log.txt"
EDU_THRESHOLD  = 0.4           # minimum educational rating to consider

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, "a"), logging.StreamHandler()]
)
log = logging.getLogger()

# ---------- SECRETS ----------
load_dotenv()
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
for name,val in {"SUPABASE_URL":SUPABASE_URL,
                 "SUPABASE_KEY":SUPABASE_KEY,
                 "GEMINI_API_KEY":GEMINI_API_KEY}.items():
    if not val:
        log.error(f"Missing env var: {name}"); exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(f"models/{MODEL_NAME}")

# ---------- PROMPT ----------
PROMPT = open("final_prompt.txt").read()

@retry(wait=wait_exponential(min=4, max=60),
       stop=stop_after_attempt(MAX_RETRIES))
def draft_mini_game(transcript:str) -> dict:
    prompt = PROMPT.replace("{{TRANSCRIPT}}", transcript)
    resp   = model.generate_content(
        prompt,
        generation_config={"response_mime_type": "application/json"}
    )
    try:
        return json.loads(resp.text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Bad JSON from Gemini: {e}")

def fetch_transcripts(limit:int=1000):
    """Grab top transcripts by video views that meet educational criteria."""
    res = supabase.rpc('fetch_top_transcripts', {'limit_count': limit}).execute()
    
    pool = []
    for r in res.data:
        txt = (r.get("transcript") or "").strip()
        if not txt:
            continue

        # Skip obvious non-edu vids
        cats = r.get("all_categories") or []
        if "not_educational" in cats:
            continue

        # JSONB rating comes in as dict or None
        rating_obj = r.get("educational_rating") or {}
        score = float(rating_obj.get("educational_rating") or 0)

        if score < EDU_THRESHOLD:
            continue

        pool.append({"video_id": r["video_id"], "transcript": txt})

    return pool

# ---------- MAIN ----------
def run(sample_size:int=SAMPLE_SIZE):
    rows = fetch_transcripts(sample_size)
    if not rows:
        log.info("No eligible transcripts 😊"); return

    log.info(f"🧠  Generating games for {len(rows)} videos …")
    for r in rows:
        vid   = r["video_id"]
        txt   = r["transcript"].strip()
        try:
            payload = draft_mini_game(txt)

            supabase.table("mini_games_duplicate").upsert({
                "video_id":            vid,
                "should_generate_game": payload["should_generate_game"],
                "skip_reason":         payload.get("skip_reason", ""),
                "concept_pool":        payload.get("concept_pool"),
                "game_choices":        payload.get("game_choices"),
                "one_cloze":           payload.get("one_cloze"),
                "one_mcq":           payload.get("one_mcq"),
                "cloze_set":           payload.get("cloze_set"),
                "mcq_set":           payload.get("mcq_set"),
                "tf_set":              payload.get("tf_set"),
            }, on_conflict="video_id").execute()

            log.info(f"✓ {vid[:8]}… → {payload['game_choices']}")
        except Exception as e:
            log.error(f"✗ {vid[:8]}… {e}")

    log.info("✅  Batch finished")

if __name__ == "__main__":
    run()