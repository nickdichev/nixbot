-- Newest open-PR update time seen during the last successful
-- reconcile; bounds the next PR listing so restarts need not
-- scan the whole open-PR backlog.
ALTER TABLE projects ADD COLUMN reconcile_watermark TIMESTAMPTZ;
