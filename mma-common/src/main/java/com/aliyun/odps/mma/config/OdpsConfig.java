package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.OdpsPartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;

import java.util.List;

//@Component
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OdpsConfig extends SourceConfig {
    @ConfigItem(desc = "maxcompute endpoint", required = true)
    public static String MC_ENDPOINT = "mc.endpoint";
    @ConfigItem(desc = "maxcompute access id", required = true)
    public static String MC_AUTH_ACCESS_ID = "mc.auth.access.id";
    @ConfigItem(desc = "maxcompute access key", required = true, type = "password")
    public static String MC_AUTH_ACCESS_KEY = "mc.auth.access.key";
    @ConfigItem(desc = "maxcompute default project(用于执行sql的project)", required = true)
    public static String MC_DEFAULT_PROJECT = "mc.default.project";
    @ConfigItem(desc = "要迁移的maxcompute projects", type = "list", required = true)
    public static String MC_PROJECTS = "mc.projects";
    @ConfigItem(desc = "maxcompute endpoint http connect timeout(单位s)", type = "int", defaultValue = "3")
    public static String MC_REST_CONN_TIMEOUT = "mc.rest.conn.timeout";
    @ConfigItem(desc = "maxcompute endpoint http read timeout(单位s)", type = "int", defaultValue = "5")
    public static String MC_REST_READ_TIMEOUT = "mc.rest.read.timeout";
    @ConfigItem(desc = "maxcompute endpoint http try times", type = "int", defaultValue = "1")
    public static String MC_REST_TRY_TIMES = "mc.rest.try.times";
    @ConfigItem(desc = "instance number of one copyTask, 仅用于\"跨region项目迁移\"", type = "int", defaultValue = "100")
    public static String COPYTASK_INS_NUM = "copytask.ins.num";
    @ConfigItem(desc = "maxcompute 迁移任务sql参数, 仅用于\"同region项目迁移\"", type = "map", defaultValue = "{\n    " +
            "\"odps.sql.hive.compatible\": \"true\"" +
            "\n}")
    public static String MC_SQL_HINTS = "mc.sql.hints";
    @ConfigItem(desc = "单个任务处理的最多分区数量, 仅用于\"同region项目迁移\"", type = "int", defaultValue = "50")
    public static final String MC_TASK_PARTITION_MAX_NUM = "mc.task.partition.max.num";


    public OdpsConfig() {
        super();
    }

    @Override
    public PartitionGrouping getPartitionGrouping() {
        return new OdpsPartitionGrouping();
    }

    @Override
    public TaskType defaultTaskType() {
        return TaskType.ODPS;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.ODPS;
    }

    public List<String> getProjects() {
        return getList(MC_PROJECTS);
    }
}