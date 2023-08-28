create table if not exists `config` (
    `id` integer auto_increment not null primary key,
    `category` varchar(255) not null,
    `name` varchar(255) not null,
    `value` text not null
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `data_source` (
    `id` integer auto_increment not null primary key,
    `name` varchar (255) not null comment '数据源名',
    `type` char (50) not null comment '数据源类型，如hive, odps',
    `last_update_time` datetime comment '最后一次更新元数据的时间',
    `db_num` integer comment 'db数量',
    `table_num` integer comment 'table数量',
    `partition_num` integer comment '分区数量',
    `create_time` datetime ,
    `update_time` datetime 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `db_model` (
    `id` integer auto_increment not null primary key,
    `source_id` integer not null comment '数据源id',
    `name` varchar (255) not null comment '数据库名',
    `status` char(50) comment '迁移状态',
    `description` varchar(255) comment '描述信息或备注',
    `owner` varchar (255) comment 'owner信息',
    `last_ddl_time` datetime comment '库最后修改时间',
    `size` bigint comment 'size in bytes',
    `num_rows` bigint comment '行数',
    `location` varchar (1024) comment '库文件目录位置',
    `updated` boolean default false comment '元数据是否有更新',
    `extra` text comment '其他信息，以json格式保存',
    `create_time` datetime ,
    `update_time` datetime 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `table_model` (
    `id` integer auto_increment not null primary key,
    `source_id` integer not null comment '数据源id',
    `db_id` integer not null comment '所属数据库的id',
    `db_name` varchar (255) not null comment '所属数据库的名字',
    `schema_name` varchar (255) comment 'odps的三层模型中的schema',
    `name` varchar (255) not null comment '表名',
    `type` varchar(255) comment '表类型, 如managed, external',
    `has_partitions` boolean not null comment '是否有分区',
    `status` char(50) comment '迁移状态',
    `size` bigint comment 'size in bytes',
    `num_rows` bigint comment '行数',
    `table_schema` mediumtext not null comment 'table schema in json',
    `owner` varchar(255) comment 'owner',
    `last_ddl_time` datetime comment '表最后修改时间',
    `location` varchar(1024) comment '表文件位置',
    `input_format` varchar(1024),
    `output_format` varchar(1024),
    `serde` varchar(255),
    `updated` boolean default false comment '元数据是否有更新',
    `extra` text comment '其他信息，以json格式保存',
    `create_time` datetime ,
    `update_time` datetime 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `partition_model` (
    `id` integer auto_increment not null primary key,
    `source_id` integer not null comment '数据源id',
    `db_id` integer not null comment '所属数据库的id',
    `table_id` integer not null comment '所属table的id',
    `db_name` varchar(255) not null comment '所属数据库的名字',
    `table_name` varchar(255) not null comment '所属table的名字',
    `value` varchar(1024) not null comment '分区值，格式为p1=v1/p2=v2',
    `status` char(50) comment '迁移状态',
    `size` bigint comment 'size in bytes',
    `num_rows` bigint comment '行数',
    `last_ddl_time` datetime comment '分区最后修改时间',
    `serde` varchar(255),
    `extra` text comment '其他信息，以json格式保存',
    `updated` boolean default false comment '元数据是否有更新',
    `create_time` datetime ,
    `update_time` datetime ,

    index (`source_id`),
    index (`db_id`),
    index (`table_id`),
    index (`db_name`),
    index (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `task` (
    `id` integer not null primary key,
    `job_id` integer not null,
    `source_id` integer not null,
    `db_id` integer not null,
    `table_id` integer not null,
    `db_name` varchar(255) not null,
    `table_name` varchar(255) not null,
    `odps_project` varchar(255) not null,
    `odps_table` varchar(255) not null,
    `type`  char(50) not null,
    `status` char(50) not null,
    `stopped` boolean default false,
    `restart` boolean default false,
    `retried_times` integer default 0 not null,
    `start_time` datetime,
    `end_time` datetime,
    `deleted` boolean default false,
    `create_time` datetime ,
    `update_time` datetime ,

    index (`job_id`),
    index (`status`),
    index (`source_id`),
    index (`db_id`),
    index (`table_id`),
    index (`db_name`),
    index (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `job` (
    `id` integer not null primary key,
    `description` varchar(255),
    `source_name` varchar (255) not null,
    `db_name` varchar (255) not null,
    `odps_project` varchar(255),
    `status` char(50) not null,
    `type` char(50) not null,
    `stopped` boolean default false,
    `restart` boolean default false,
    `config` text,
    `deleted` boolean default false,
    `create_time` datetime ,
    `update_time` datetime 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `task_partition` (
    `id` integer auto_increment not null primary key,
    `job_id` integer not null,
    `task_id` integer not null,
    `partition_id` integer not null,
    `create_time` datetime ,
    `update_time` datetime ,
    index (job_id),
    index (task_id),
    index (partition_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create table if not exists `task_log` (
    `id` integer auto_increment not null primary key,
    `task_id` integer not null,
    `status` char(50) not null,
    `action` text not null,
    `msg` text not null,
    `create_time` datetime ,
    `update_time` datetime ,
    index (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create or replace view task_id_group_by_table as
    select min(task.id) as task_id from task join job on task.job_id=job.id
    where (task.status='INIT' or task.restart=1)
    and task.stopped=0 and task.deleted=0 and job.stopped=0 and job.deleted=0
    group by (task.table_name);



