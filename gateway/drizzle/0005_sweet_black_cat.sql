CREATE TABLE `project_settings_requests` (
	`request_id` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`executor_id` text NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`error` text,
	`result` text,
	`received_at` real NOT NULL
);
--> statement-breakpoint
CREATE TABLE `settings_commands` (
	`request_id` text PRIMARY KEY NOT NULL,
	`executor_id` text NOT NULL,
	`kind` text NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`error` text,
	`received_at` real NOT NULL
);
