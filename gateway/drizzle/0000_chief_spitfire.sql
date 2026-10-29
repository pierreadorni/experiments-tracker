CREATE TABLE `artifacts` (
	`id` text PRIMARY KEY NOT NULL,
	`experiment_id` text NOT NULL,
	`name` text NOT NULL,
	`sha256` text NOT NULL,
	`size` integer NOT NULL,
	`media_type` text NOT NULL,
	`object_key` text NOT NULL,
	`available` integer DEFAULT 0 NOT NULL,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE INDEX `idx_artifacts_experiment` ON `artifacts` (`experiment_id`);--> statement-breakpoint
CREATE TABLE `commands` (
	`request_id` text PRIMARY KEY NOT NULL,
	`experiment_id` text NOT NULL,
	`executor_id` text NOT NULL,
	`kind` text NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`received_at` real NOT NULL,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE INDEX `idx_commands_executor_state` ON `commands` (`executor_id`,`state`);--> statement-breakpoint
CREATE TABLE `events` (
	`id` text PRIMARY KEY NOT NULL,
	`experiment_id` text NOT NULL,
	`revision` integer NOT NULL,
	`received_at` real NOT NULL,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `executor_status` (
	`id` text PRIMARY KEY NOT NULL,
	`payload` text NOT NULL,
	`received_at` real NOT NULL
);
--> statement-breakpoint
CREATE TABLE `inbox` (
	`id` text PRIMARY KEY NOT NULL,
	`experiment_id` text NOT NULL,
	`kind` text NOT NULL,
	`text` text NOT NULL,
	`created_at` real NOT NULL,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE TABLE `notes` (
	`id` text PRIMARY KEY NOT NULL,
	`experiment_id` text NOT NULL,
	`author` text NOT NULL,
	`text` text NOT NULL,
	`created_at` real NOT NULL,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE INDEX `idx_notes_experiment` ON `notes` (`experiment_id`);--> statement-breakpoint
CREATE TABLE `projections` (
	`id` text PRIMARY KEY NOT NULL,
	`revision` integer NOT NULL,
	`payload` text NOT NULL,
	`updated_at` real NOT NULL
);
