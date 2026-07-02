-- Evaluation identity for branch-specific flake attribute selection.
-- JSON array string: [flake_dir, lock_file, attribute].
ALTER TABLE builds
ADD COLUMN eval_key TEXT NOT NULL DEFAULT '[".","flake.lock","checks"]';

CREATE INDEX builds_eval_identity_idx ON builds (project_id, tree_hash, eval_key);
