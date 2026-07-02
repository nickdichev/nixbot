-- Effects are branch-sensitive even when the build artifacts are reused
-- across branches. Track each triggering ref separately so, for example,
-- a production push can deploy after reusing an already-built main commit.
CREATE TABLE build_effect_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    build_id BIGINT NOT NULL REFERENCES builds (id) ON DELETE CASCADE,
    commit_sha TEXT NOT NULL,
    branch TEXT NOT NULL,
    -- 0 means "not a pull request"; PR numbers are positive.
    pr_number BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (build_id, branch, pr_number)
);

CREATE INDEX build_effect_runs_build_idx ON build_effect_runs (build_id);

ALTER TABLE build_effects
ADD COLUMN run_id BIGINT REFERENCES build_effect_runs (id) ON DELETE CASCADE;

INSERT INTO build_effect_runs (build_id, commit_sha, branch, pr_number)
SELECT DISTINCT b.id,
       COALESCE(b.effects_commit_sha, b.commit_sha),
       COALESCE(b.effects_branch, b.branch),
       COALESCE(b.effects_pr_number, b.pr_number, 0)
FROM builds b
JOIN build_effects e ON e.build_id = b.id
ON CONFLICT (build_id, branch, pr_number) DO NOTHING;

UPDATE build_effects e
SET run_id = r.id
FROM builds b
JOIN build_effect_runs r ON r.build_id = b.id
WHERE e.build_id = b.id
  AND e.run_id IS NULL
  AND r.commit_sha = COALESCE(b.effects_commit_sha, b.commit_sha)
  AND r.branch = COALESCE(b.effects_branch, b.branch)
  AND r.pr_number = COALESCE(b.effects_pr_number, b.pr_number, 0);

ALTER TABLE build_effects
DROP CONSTRAINT build_effects_build_id_name_key;

CREATE UNIQUE INDEX build_effects_run_name_idx
ON build_effects (run_id, name)
WHERE run_id IS NOT NULL;
