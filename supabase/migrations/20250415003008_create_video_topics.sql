-- Create a table for video categories
CREATE TABLE
  video_topics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic TEXT NOT NULL,
    confidence DECIMAL(4, 3) NOT NULL CHECK (
      confidence >= 0.0000
      AND confidence <= 1.0000
    ),
    video_id UUID NOT NULL REFERENCES videos (id)
  );

CREATE INDEX video_topics_topic_confidence_idx ON video_topics (topic, confidence);

-- Copy data from the videos table to the video_topics table
INSERT INTO video_topics (topic, confidence, video_id)
SELECT
  elem->>'topic',
  (elem->>'confidence')::DECIMAL(4, 3),
  v.id AS video_id
FROM
  videos v,
  LATERAL jsonb_array_elements (v.topics) AS elem;
