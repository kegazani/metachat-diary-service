-- Create keyspace for metachat
CREATE KEYSPACE IF NOT EXISTS metachat 
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

-- Use the keyspace
USE metachat;

-- Create diary entries table
CREATE TABLE IF NOT EXISTS diary_entries (
  id text PRIMARY KEY,
  user_id text,
  title text,
  content text,
  token_count int,
  session_id text,
  tags set<text>,
  created_at timestamp,
  updated_at timestamp
);

-- Create index on user_id for querying by user
CREATE INDEX IF NOT EXISTS diary_entries_user_id_idx ON diary_entries (user_id);

-- Create index on session_id for querying by session
CREATE INDEX IF NOT EXISTS diary_entries_session_id_idx ON diary_entries (session_id);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS diary_entries_created_at_idx ON diary_entries (created_at);

-- Create diary sessions table
CREATE TABLE IF NOT EXISTS diary_sessions (
  id text PRIMARY KEY,
  user_id text,
  title text,
  description text,
  started_at timestamp,
  ended_at timestamp,
  status text,
  entry_count int,
  created_at timestamp,
  updated_at timestamp
);

-- Create index on user_id for querying sessions by user
CREATE INDEX IF NOT EXISTS diary_sessions_user_id_idx ON diary_sessions (user_id);

-- Create index on status for querying by status
CREATE INDEX IF NOT EXISTS diary_sessions_status_idx ON diary_sessions (status);

-- Create index on started_at for time-based queries
CREATE INDEX IF NOT EXISTS diary_sessions_started_at_idx ON diary_sessions (started_at);

-- Create diary entry read model table (for queries)
CREATE TABLE IF NOT EXISTS diary_entry_read_models (
  id text PRIMARY KEY,
  user_id text,
  title text,
  content text,
  token_count int,
  session_id text,
  tags set<text>,
  created_at timestamp,
  updated_at timestamp,
  source text static
);

-- Create index on user_id for querying read models by user
CREATE INDEX IF NOT EXISTS diary_entry_read_models_user_id_idx ON diary_entry_read_models (user_id);

-- Create diary session read model table (for queries)
CREATE TABLE IF NOT EXISTS diary_session_read_models (
  id text PRIMARY KEY,
  user_id text,
  title text,
  description text,
  started_at timestamp,
  ended_at timestamp,
  status text,
  entry_count int,
  token_count int,
  created_at timestamp,
  updated_at timestamp,
  source text static
);

-- Create index on user_id for querying session read models by user
CREATE INDEX IF NOT EXISTS diary_session_read_models_user_id_idx ON diary_session_read_models (user_id);

-- Create event store table for event sourcing
CREATE TABLE IF NOT EXISTS diary_events (
  aggregate_id text,
  version bigint,
  event_type text,
  event_data text,
  created_at timestamp,
  PRIMARY KEY (aggregate_id, version)
) WITH CLUSTERING ORDER BY (version ASC);

-- Create index on event_type for querying by event type
CREATE INDEX IF NOT EXISTS diary_events_event_type_idx ON diary_events (event_type);

-- Create index on created_at for time-based queries
CREATE INDEX IF NOT EXISTS diary_events_created_at_idx ON diary_events (created_at);

-- Create snapshot table for performance optimization
CREATE TABLE IF NOT EXISTS diary_snapshots (
  aggregate_id text PRIMARY KEY,
  version bigint,
  snapshot_data text,
  created_at timestamp
);

-- Create index on created_at for snapshot cleanup
CREATE INDEX IF NOT EXISTS diary_snapshots_created_at_idx ON diary_snapshots (created_at);

-- Insert some sample data for testing (optional)
INSERT INTO diary_sessions (id, user_id, title, description, started_at, ended_at, status, entry_count, created_at, updated_at)
VALUES (
  'session-1',
  'user-1',
  'My First Diary Session',
  'A sample diary session for testing',
  toTimestamp(now()),
  null,
  'active',
  0,
  toTimestamp(now()),
  toTimestamp(now())
) IF NOT EXISTS;

INSERT INTO diary_entries (id, user_id, title, content, token_count, session_id, tags, created_at, updated_at)
VALUES (
  'entry-1',
  'user-1',
  'My First Diary Entry',
  'This is my first diary entry. It contains some sample content to test the system.',
  25,
  'session-1',
  {'personal', 'first', 'test'},
  toTimestamp(now()),
  toTimestamp(now())
) IF NOT EXISTS;