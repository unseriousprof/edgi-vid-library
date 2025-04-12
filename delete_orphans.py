import os
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables from .env file
load_dotenv()

# Get Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase client
try:
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("Successfully connected to Supabase.")
except Exception as e:
    print(f"Error connecting to Supabase: {e}")
    exit(1)

# Step 1: Get all tiktok_ids from the videos table
try:
    response = supabase.table("videos").select("tiktok_id").execute()
    tiktok_ids = {item["tiktok_id"] for item in response.data}
    print(f"Found {len(tiktok_ids)} tiktok_ids in the videos table.")
except Exception as e:
    print(f"Error fetching tiktok_ids from videos table: {e}")
    exit(1)

# Step 2: Get all files in the videos bucket
try:
    storage_files = supabase.storage.from_("videos").list()
    print(f"Found {len(storage_files)} files in the videos bucket.")
except Exception as e:
    print(f"Error listing files in videos bucket: {e}")
    exit(1)

# Step 3: Identify orphaned files (files in Storage without a matching tiktok_id)
orphaned_files = []
for file in storage_files:
    file_name = file["name"]
    # Extract tiktok_id from file name (remove .mp4 extension)
    tiktok_id = file_name.replace(".mp4", "")
    if tiktok_id not in tiktok_ids:
        orphaned_files.append(file_name)

print(f"Found {len(orphaned_files)} orphaned files to delete.")

# Step 4: Delete orphaned files
if not orphaned_files:
    print("No orphaned files to delete. Exiting.")
    exit(0)

for file_name in orphaned_files:
    try:
        supabase.storage.from_("videos").remove([file_name])
        print(f"Deleted {file_name} from Storage.")
    except Exception as e:
        print(f"Error deleting {file_name}: {e}")

# Step 5: Verify the deletion
try:
    storage_files_after = supabase.storage.from_("videos").list()
    print(f"After deletion, {len(storage_files_after)} files remain in the videos bucket.")
except Exception as e:
    print(f"Error listing files after deletion: {e}")
    exit(1)

print("Script completed successfully.")