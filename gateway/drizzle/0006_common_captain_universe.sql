CREATE TABLE `push_follows` (
	`subscription_id` text NOT NULL,
	`experiment_id` text NOT NULL,
	`created_at` real NOT NULL,
	FOREIGN KEY (`subscription_id`) REFERENCES `push_subscriptions`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE UNIQUE INDEX `idx_push_follows_identity` ON `push_follows` (`subscription_id`,`experiment_id`);--> statement-breakpoint
CREATE TABLE `push_outbox` (
	`id` text PRIMARY KEY NOT NULL,
	`subscription_id` text NOT NULL,
	`experiment_id` text NOT NULL,
	`revision` integer NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`next_attempt_at` real NOT NULL,
	`lease` text,
	`last_status` integer,
	`created_at` real NOT NULL,
	FOREIGN KEY (`subscription_id`) REFERENCES `push_subscriptions`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE INDEX `idx_push_outbox_pending` ON `push_outbox` (`state`,`next_attempt_at`);--> statement-breakpoint
CREATE TABLE `push_receipts` (
	`request_id` text PRIMARY KEY NOT NULL,
	`owner` text NOT NULL,
	`payload_hash` text NOT NULL
);
--> statement-breakpoint
CREATE TABLE `push_subscriptions` (
	`id` text PRIMARY KEY NOT NULL,
	`owner` text NOT NULL,
	`subscription` text NOT NULL,
	`updated_at` real NOT NULL
);
