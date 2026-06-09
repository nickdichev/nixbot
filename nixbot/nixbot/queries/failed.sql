-- Failed-build cache and failed-status records (failed_builds.py,
-- status.py, recovery.py retention).

-- name: FailedBuildByDrv :one
SELECT derivation, timestamp, url FROM failed_builds
WHERE project_id = $1 AND derivation = $2;

-- name: UpsertFailedBuild :exec
INSERT INTO failed_builds (project_id, derivation, timestamp, url)
VALUES ($1, $2, $3, $4)
ON CONFLICT (project_id, derivation)
DO UPDATE SET timestamp = EXCLUDED.timestamp, url = EXCLUDED.url;

-- name: UpsertFailedStatus :exec
INSERT INTO failed_statuses (revision, status_name, timestamp)
VALUES ($1, $2, $3)
ON CONFLICT (revision, status_name)
DO UPDATE SET timestamp = EXCLUDED.timestamp;

-- name: FailedStatusNames :many
SELECT status_name FROM failed_statuses WHERE revision = $1;

-- name: ClearFailedStatus :exec
DELETE FROM failed_statuses WHERE revision = $1 AND status_name = $2;

-- name: PruneOldFailedStatuses :exec
DELETE FROM failed_statuses
WHERE to_timestamp(timestamp) < now() - make_interval(days => sqlc.arg(retention_days)::int);

-- name: PruneOldFailedBuilds :exec
DELETE FROM failed_builds
WHERE to_timestamp(timestamp) < now() - make_interval(days => sqlc.arg(retention_days)::int);
