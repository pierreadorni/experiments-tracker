CREATE TABLE `autopilot_commands` (
	`request_id` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`executor_id` text NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`error` text,
	`received_at` real NOT NULL
);
--> statement-breakpoint
CREATE TABLE `autopilot_documents` (
	`identity` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`run_id` text NOT NULL,
	`name` text NOT NULL,
	`version` integer NOT NULL,
	`object_key` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `autopilot_views` (
	`project_id` text PRIMARY KEY NOT NULL,
	`executor_id` text NOT NULL,
	`run_id` text NOT NULL,
	`revision` integer NOT NULL,
	`payload` text NOT NULL,
	`observed_at` real NOT NULL
);
