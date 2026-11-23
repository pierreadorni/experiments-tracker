-- Data only: copy follows by experiment id into follows by projection key.
INSERT OR IGNORE INTO `relay_follows` (`subscription_id`, `key`, `created_at`)
SELECT `subscription_id`, 'experiment:' || `experiment_id`, `created_at` FROM `push_follows`;
