CREATE TABLE `project_requests` (
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
CREATE UNIQUE INDEX `project_requests_project_id_unique` ON `project_requests` (`project_id`);