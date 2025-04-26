import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment secrets
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not all([SUPABASE_URL, SUPABASE_KEY]):
    print("Missing environment variables")
    exit(1)

# Set up Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# List of video IDs to fix
video_ids = [
    "983f9e40-f552-41fa-a845-a88723cdfeef",
    "8c04b35c-25a9-43d0-910c-1d13c2d0c65d",
    "2b13d90f-edd8-4231-9b5f-61e73bbaf8c8",
    "5a90c572-9856-46f3-860d-c4ccc13c7eb4",
    "de09e4f9-1840-412f-8a03-28f56c9d9123"
]

# Re-upload each preview image with the correct content type
for video_id in video_ids:
    preview_storage_path = f"previews/{video_id}.jpg"
    # Download the existing file
    file_data = supabase.storage.from_("assets").download(preview_storage_path)
    # Update (overwrite) the file with correct content type
    supabase.storage.from_("assets").update(
        preview_storage_path,
        file_data,
        file_options={"content-type": "image/jpeg"}
    )
    print(f"Fixed content type for {video_id}.jpg")

print("All previews updated successfully!")