CREATE TABLE `relay_commands` (
	`request_id` text PRIMARY KEY NOT NULL,
	`executor_id` text NOT NULL,
	`kind` text NOT NULL,
	`target` text,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`result` text,
	`error` text,
	`received_at` real NOT NULL,
	`settled_at` real
);
--> statement-breakpoint
CREATE INDEX `idx_relay_commands_state` ON `relay_commands` (`executor_id`,`state`,`received_at`);--> statement-breakpoint
CREATE TABLE `relay_follows` (
	`subscription_id` text NOT NULL,
	`key` text NOT NULL,
	`created_at` real NOT NULL,
	FOREIGN KEY (`subscription_id`) REFERENCES `push_subscriptions`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `idx_relay_follows_identity` ON `relay_follows` (`subscription_id`,`key`);--> statement-breakpoint
CREATE TABLE `relay_projections` (
	`key` text PRIMARY KEY NOT NULL,
	`executor_id` text NOT NULL,
	`revision` integer NOT NULL,
	`payload` text,
	`digest` text NOT NULL,
	`notify_fingerprint` text,
	`updated_at` real NOT NULL
);
--> statement-breakpoint
CREATE TABLE `relay_push_outbox` (
	`id` text PRIMARY KEY NOT NULL,
	`subscription_id` text NOT NULL,
	`key` text NOT NULL,
	`revision` integer NOT NULL,
	`payload` text NOT NULL,
	`state` text DEFAULT 'pending' NOT NULL,
	`attempts` integer DEFAULT 0 NOT NULL,
	`next_attempt_at` real NOT NULL,
	`lease` text,
	`last_status` integer,
	`created_at` real NOT NULL,
	FOREIGN KEY (`subscription_id`) REFERENCES `push_subscriptions`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `idx_relay_push_outbox_pending` ON `relay_push_outbox` (`state`,`next_attempt_at`);