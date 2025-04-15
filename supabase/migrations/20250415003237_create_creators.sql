-- Create a table for video categories
CREATE TABLE
  creators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username TEXT NOT NULL UNIQUE,
    username_length INTEGER
  );

-- Copy data from the videos table to the creators table
INSERT INTO creators (username, username_length)
SELECT creator_username, LENGTH(creator_username)
FROM videos v
GROUP BY creator_username;

-- Add a creator_id column to the videos table
ALTER TABLE videos ADD COLUMN creator_id UUID REFERENCES creators(id);

-- Update the creator_id column with the corresponding creator_id from the creators table
UPDATE videos v
SET creator_id = (
  SELECT id
  FROM creators c
  WHERE c.username = v.creator_username
);

-- Make the creator_id column required in the videos table
ALTER TABLE videos ALTER COLUMN creator_id SET NOT NULL;
