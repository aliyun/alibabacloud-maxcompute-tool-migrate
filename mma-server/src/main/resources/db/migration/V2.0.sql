alter table partition_model add column schema_name varchar (255) comment 'odps的三层模型中的schema' after db_name;
alter table task add column schema_name varchar (255) after db_name;
alter table task add column odps_schema varchar (255) after odps_project;
alter table task add column `batch_id` integer not null after job_id;
alter table job add column `timer` text after restart;
alter table job add column  `last_batch` integer default 0 after timer;
alter table job add column odps_schema varchar (255) after odps_project;


alter table task add column `running` boolean default false after retried_times;

update task set running=1 where status != 'INIT' and status != 'DONE' and status != 'SCHEMA_FAILED' and status != 'DATA_FAILED' and status != 'VERIFICATION_FAILED' and stopped=0 and deleted=0;

ALTER TABLE `partition_model` ADD INDEX `idx_partition_source` (`source_id`, `db_id`, `table_id`, `status`);
