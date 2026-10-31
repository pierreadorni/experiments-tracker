CREATE TABLE `maintenance_commands` (
	`request_id` text PRIMARY KEY NOT NULL,
	`executor_id` text NOT NULL,
	`release_id` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`error` text,
	`received_at` real NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `one_pending_maintenance` ON `maintenance_commands` (`executor_id`) WHERE "maintenance_commands"."state" = 'pending';--> statement-breakpoint
CREATE TABLE `maintenance_status` (
	`executor_id` text PRIMARY KEY NOT NULL,
	`payload` text NOT NULL,
	`observed_at` real NOT NULL
);
