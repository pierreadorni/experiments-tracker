CREATE TABLE `project_memory` (
	`project_id` text PRIMARY KEY NOT NULL,
	`executor_id` text NOT NULL,
	`revision` integer NOT NULL,
	`payload` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `project_memory_commands` (
	`request_id` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`executor_id` text NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`error` text,
	`received_at` real NOT NULL
);
--> statement-breakpoint
CREATE TABLE `project_memory_reports` (
	`identity` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`version` integer NOT NULL,
	`payload` text NOT NULL
);
