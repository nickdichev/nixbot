-- Scheduled-effect runs have no build row, so the build/attribute
-- triggers (migration 0005) never fire for them and the repo Schedules
-- table and run-history list stay stale until a manual refresh. Emit on
-- the same build_events channel; the payload carries project_id (for
-- visibility filtering) and run_id, but no build_id, so listeners can
-- tell run events from build/attribute events. schedule_name/effect are
-- repo-controlled, so truncate them (cf. migration 0012).

CREATE FUNCTION notify_scheduled_run_status() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status IS NOT DISTINCT FROM NEW.status THEN
        RETURN NEW;
    END IF;
    PERFORM pg_notify('build_events', json_build_object(
        'project_id', NEW.project_id,
        'run_id', NEW.id,
        'schedule_name', left(NEW.schedule_name, 256),
        'effect', left(NEW.effect, 256),
        'status', NEW.status)::text);
    RETURN NEW;
END $$ LANGUAGE plpgsql;

CREATE TRIGGER scheduled_effect_runs_status_notify
AFTER INSERT OR UPDATE ON scheduled_effect_runs
FOR EACH ROW EXECUTE FUNCTION notify_scheduled_run_status();
