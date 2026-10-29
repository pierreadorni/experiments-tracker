CREATE TABLE `conversation` (
	`id` text PRIMARY KEY NOT NULL,
	`experiment_id` text NOT NULL,
	`seq` integer NOT NULL,
	`revision` integer NOT NULL,
	`payload` text NOT NULL,
	FOREIGN KEY (`experiment_id`) REFERENCES `projections`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE UNIQUE INDEX `idx_conversation_experiment_seq` ON `conversation` (`experiment_id`,`seq`);