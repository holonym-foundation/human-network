-- This file should undo anything in `up.sql`
ALTER TABLE peer_reachability_tcp DROP COLUMN IF EXISTS version;
