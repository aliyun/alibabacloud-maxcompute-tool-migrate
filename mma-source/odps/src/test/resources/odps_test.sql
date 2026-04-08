-- Delta table
create table pk
(
    i int not null,
    primary key (i)
) tblproperties ('transactional' = 'true');

insert into pk
values (1);

-- No partition table test storage tier
create table no_pt_storage_tier
(
    s string
) tblproperties ("storagetier" = "lowfrequency");

insert into no_pt_storage_tier
values ('s');

-- Partition table test storage tier
drop table if exists pt_storage_tier;

create table pt_storage_tier
(
    s string
) partitioned by (pt string);

alter table pt_storage_tier
    add partition (pt = 'p1');

alter table pt_storage_tier partition (pt='p1') set partitionproperties('storagetier'='lowfrequency');

insert into table pt_storage_tier partition (pt = 'p1')
values ('s');

-- Partition table test complex storage tier config
drop table if exists pt_storage_tier_complex;

create table pt_storage_tier_complex
(
    s string
) partitioned by (pt string) tblproperties ('lifecycle_config' =
        '{"TierToLowFrequency":{"DaysAfterLastModificationGreaterThan":1}}');

alter table pt_storage_tier_complex
    add partition (pt = 'p1');

alter table pt_storage_tier_complex
    add partition (pt = 'p2');

insert into table pt_storage_tier_complex partition (pt = 'p1')
values ('s');

insert into table pt_storage_tier_complex partition (pt = 'p2')
values ('s');

alter table pt_storage_tier_complex partition (pt='p2') set partitionproperties('storagetier'='longterm');

-- Partition table test lifecycle disable
create table lifecycle_disable(s string);

alter table lifecycle_disable disable lifecycle;

create table pt_lifecycle
(
    s string
) partitioned by (pt string);

alter table pt_lifecycle
    add partition (pt = 'p1');
alter table pt_lifecycle
    add partition (pt = 'p2');

alter table pt_lifecycle
    partition (pt = 'p1') disable lifecycle;

-- Hash Cluster Table
create table cluster_table
(
    s string
) clustered by (s) sorted by (s) into 512 buckets;

insert overwrite table cluster_table select * from pk;

create table cluster_pt_table
(
    s string
) partitioned by (pt string)
    clustered by (s)
        sorted by (s)
        into 512 buckets;

alter table cluster_pt_table add partition (pt = 'p1');

insert overwrite table cluster_pt_table partition (pt='p1') select * from pk;

select * from cluster_pt_table where pt= 'p1';