-- Create a table for video difficulty levels
CREATE TYPE difficulty_level AS ENUM ('beginner', 'intermediate', 'advanced');

CREATE TABLE
  video_difficulties (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    difficulty difficulty_level NOT NULL,
    confidence DECIMAL(4, 3) NOT NULL CHECK (
      confidence >= 0.0000
      AND confidence <= 1.0000
    ),
    video_id UUID NOT NULL REFERENCES videos (id)
  );

-- Copy data from the videos table to the video_difficulties table
INSERT INTO video_difficulties (difficulty, confidence, video_id)
SELECT
  CAST(v.difficulty_level->>'level' AS difficulty_level),
  (v.difficulty_level->>'confidence')::DECIMAL(4, 3),
  v.id AS video_id
FROM videos v
WHERE v.difficulty_level->>'level' IS NOT NULL;
