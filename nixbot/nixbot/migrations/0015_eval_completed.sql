-- Marks builds whose complete eval result is recorded in
-- build_attributes, so a later build of the same tree can skip
-- re-running nix-eval-jobs and reconstruct the jobs from the rows.
ALTER TABLE builds ADD COLUMN eval_completed boolean NOT NULL DEFAULT false;
