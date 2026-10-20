-- Additive topic graph: no extension, table rewrite, or graph database required.
-- Run independently of the full legacy schema provisioning script.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS knowledge_topics (
    url TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    language TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS video_topics (
    video_id VARCHAR(20) NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    topic_url TEXT NOT NULL REFERENCES knowledge_topics(url),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(video_id, topic_url)
);
CREATE TABLE IF NOT EXISTS channel_topics (
    channel_id VARCHAR(50) NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    topic_url TEXT NOT NULL REFERENCES knowledge_topics(url),
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(channel_id, topic_url)
);
CREATE TABLE IF NOT EXISTS youtube_topic_sync (
    kind TEXT NOT NULL CHECK (kind IN ('video', 'channel')),
    resource_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('topics', 'empty', 'unavailable')),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token UUID,
    leased_until TIMESTAMPTZ,
    PRIMARY KEY(kind, resource_id)
);
CREATE INDEX IF NOT EXISTS idx_video_topics_topic ON video_topics(topic_url, video_id);
CREATE INDEX IF NOT EXISTS idx_channel_topics_topic ON channel_topics(topic_url, channel_id);
CREATE INDEX IF NOT EXISTS idx_topic_sync_due ON youtube_topic_sync(kind, next_attempt_at, resource_id);

COMMENT ON TABLE knowledge_topics IS 'Canonical Wikipedia URLs reported by YouTube topicDetails.topicCategories.';
COMMENT ON TABLE youtube_topic_sync IS 'Observed topic coverage and expiring owned leases; absence, empty topics and unavailable resources are distinct.';
COMMIT;
