-- Create a table for video categories
CREATE TABLE
  video_categories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    category TEXT NOT NULL,
    confidence DECIMAL(4, 3) NOT NULL CHECK (
      confidence >= 0.0000
      AND confidence <= 1.0000
    ),
    video_id UUID NOT NULL REFERENCES videos (id)
  );

CREATE INDEX video_categories_category_confidence_idx ON video_categories (category, confidence);

-- Copy data from the videos table to the video_categories table
INSERT INTO video_categories (category, confidence, video_id)
SELECT
  elem->>'tag',
  (elem->>'confidence')::DECIMAL(4, 3),
  v.id AS video_id
FROM
  videos v,
  LATERAL jsonb_array_elements (v.categories) AS elem;
