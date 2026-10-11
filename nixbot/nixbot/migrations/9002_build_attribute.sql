-- The flake attribute a build covers, resolved once from the repository's
-- attribute rules. Build identity is project, tree hash and attribute:
-- the same tree built under another attribute is a different build.
ALTER TABLE builds ADD COLUMN attribute TEXT NOT NULL DEFAULT 'checks';
CREATE INDEX builds_identity_idx ON builds (project_id, tree_hash, attribute);
