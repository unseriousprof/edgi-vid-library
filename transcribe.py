import os
import logging
from faster_whisper import WhisperModel
from dotenv import load_dotenv
from supabase import create_client

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transcriber")

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Connect to Supabase
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Set video folder
video_folder = os.path.expanduser("~/Documents/video library")

# Load faster-whisper medium model
logger.info("Loading faster-whisper 'medium' model...")
# Check if running on Apple Silicon
is_apple_silicon = 'arm64' in os.uname().machine if hasattr(os, 'uname') else False
compute_type = "int8"

model = WhisperModel("medium", compute_type=compute_type)
logger.info(f"faster-whisper model loaded (compute_type: {compute_type})")

def transcribe_videos(limit=5):
    response = supabase.table("videos").select("id, tiktok_id, video_file, duration").eq("status", "downloaded").limit(limit).execute()
    videos = response.data
    logger.info(f"Found {len(videos)} video(s) to transcribe")

    for video in videos:
        try:
            video_id = video["id"]
            tiktok_id = video["tiktok_id"]
            video_path = video["video_file"]
            logger.info(f"🎧 Transcribing {tiktok_id} → {os.path.basename(video_path)}")

            if not os.path.exists(video_path):
                raise FileNotFoundError(f"File not found: {video_path}")

            # Transcribe video directly with faster-whisper
            segments, info = model.transcribe(video_path, beam_size=5)
            
            # Convert segments iterator to list and process
            segments = list(segments)  # Force it to be a list
            transcript = " ".join([segment.text.strip() for segment in segments])
            language = info.language
            
            # Calculate duration from segments or use existing
            duration = int(segments[-1].end) if segments else video.get("duration", 0)

            # Save result to Supabase
            supabase.table("videos").update({
                "transcript": transcript,
                "language": language,
                "duration": duration,
                "status": "transcribed"
            }).eq("id", video_id).execute()

            logger.info(f"✅ Transcribed: {tiktok_id}")

        except Exception as e:
            logger.error(f"❌ Error transcribing {video.get('tiktok_id', 'unknown')}: {e}")
            supabase.table("videos").update({
                "status": "error",
                "processing_errors": {
                    "stage": "transcription",
                    "error": str(e)
                }
            }).eq("id", video["id"]).execute()

if __name__ == "__main__":
    transcribe_videos(limit=5)