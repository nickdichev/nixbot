-- The lock a scheduled effect declares, as discovered from the flake.
-- Due runs queue under the same effect-lock key as onPush/onEvent
-- effects, so they serialize with them. NULL: no lock.
ALTER TABLE scheduled_effects ADD COLUMN lock TEXT;
