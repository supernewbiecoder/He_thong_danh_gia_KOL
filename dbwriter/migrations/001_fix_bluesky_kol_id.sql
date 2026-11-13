-- One-off fix for rows stored before the collector attributed Bluesky engagement correctly.
-- Before: like / repost / reply / follow were stored with kol_id = the account that DID the action.
-- After:  they belong to the account that RECEIVED it (author of the target post, followed account).
-- parent_id already holds the target, so the right kol_id can be rebuilt from it.
--
--   PRIMARY=$(kubectl -n kol-system get pods -l cnpg.io/instanceRole=primary -o name)
--   kubectl -n kol-system exec -i "$PRIMARY" -c postgres -- psql -U postgres -d kol < dbwriter/migrations/001_fix_bluesky_kol_id.sql
--
-- Docker Compose: docker exec -i kol-postgres psql -U kol_admin -d kol < dbwriter/migrations/001_fix_bluesky_kol_id.sql
-- Safe to run twice.
BEGIN;

UPDATE kol.social_events
SET kol_id = 'bluesky:' || split_part(substr(parent_id, 6), '/', 1)
WHERE platform = 'bluesky'
  AND event_type IN ('like', 'repost', 'reply')
  AND parent_id LIKE 'at://did:%'
  AND kol_id <> 'bluesky:' || split_part(substr(parent_id, 6), '/', 1);

UPDATE kol.social_events
SET kol_id = 'bluesky:' || parent_id
WHERE platform = 'bluesky'
  AND event_type = 'follow'
  AND parent_id LIKE 'did:%'
  AND kol_id <> 'bluesky:' || parent_id;

-- Scores were computed from the wrong attribution: drop them, Module 3 rebuilds them from new events.
TRUNCATE kol.kol_scores, kol.kol_score_history;

COMMIT;
