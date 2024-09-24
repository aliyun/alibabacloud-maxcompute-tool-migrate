package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.OdpsPartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;
import com.aliyun.odps.mma.util.StringUtils;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

//@Component
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OdpsConfig extends SourceConfig {
    @ConfigItem(desc = "maxcompute endpoint", required = true)
    public static String MC_ENDPOINT = "mc.endpoint";
    //@ConfigItem(desc = "vpc endpoint")
    public static String MC_DATA_ENDPOINT = "mc.data.endpoint";
    //@ConfigItem(desc = "tunnel endpoint")
    public static String MC_TUNNEL_ENDPOINT = "mc.tunnel.endpoint";
    @ConfigItem(desc = "maxcompute access id", required = true)
    public static String MC_AUTH_ACCESS_ID = "mc.auth.access.id";
    @ConfigItem(desc = "maxcompute access key", required = true, type = "password")
    public static String MC_AUTH_ACCESS_KEY = "mc.auth.access.key";
    @ConfigItem(desc = "maxcompute default project(用于执行sql的project)", required = true)
    public static String MC_DEFAULT_PROJECT = "mc.default.project";
    @ConfigItem(desc = "要迁移的maxcompute projects", type = "list", required = true)
    public static String MC_PROJECTS = "mc.src.projects";
    @ConfigItem(desc = "maxcompute endpoint http connect timeout(单位s)", type = "int", defaultValue = "3")
    public static String MC_REST_CONN_TIMEOUT = "mc.rest.conn.timeout";
    @ConfigItem(desc = "maxcompute endpoint http read timeout(单位s)", type = "int", defaultValue = "5")
    public static String MC_REST_READ_TIMEOUT = "mc.rest.read.timeout";
    @ConfigItem(desc = "maxcompute endpoint http try times", type = "int", defaultValue = "1")
    public static String MC_REST_TRY_TIMES = "mc.rest.try.times";
//    @ConfigItem(desc = "instance number of one copyTask", type = "int", defaultValue = "100")
    public static String COPYTASK_INS_NUM = "copytask.ins.num";
//    @ConfigItem(desc = "coppytask direction", defaultValue = "EXPORT", enums = {"EXPORT", "IMPORT"})
    public static String COPYTASK_DIRECTION = "copytask.direction";
    @ConfigItem(desc = "oss internal endpoint")
    public static final String OSS_ENDPOINT_INTERNAL = "oss.endpoint.internal";
    @ConfigItem(desc = "oss external endpoint" )
    public static final String OSS_ENDPOINT_EXTERNAL = "oss.endpoint.external";
    @ConfigItem(desc = "oss bucket")
    public static final String OSS_BUCKET = "oss.bucket";
    @ConfigItem(desc = "oss access id")
    public static final String OSS_AUTH_ACCESS_ID = "oss.auth.access.id";
    @ConfigItem(desc = "oss access key", type = "password")
    public static final String OSS_AUTH_ACCESS_KEY = "oss.auth.access.key";
    @ConfigItem(desc = "maxcompute 迁移任务sql参数", type = "map", defaultValue = "{\n    " +
            "\"odps.sql.hive.compatible\": \"true\"" +
            "\n}")
    public static String MC_SQL_HINTS = "mc.sql.hints";
    @ConfigItem(desc = "单个任务处理的最多分区数量", type = "int", defaultValue = "50")
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

    public String getOssPathOfExternalTable(String dbName, String schemaName, String tableName) {
        String endpoint = this.getConfig(OSS_ENDPOINT_INTERNAL);
        String accessId = this.getConfig(OSS_AUTH_ACCESS_ID);
        String accessKey = this.getConfig(OSS_AUTH_ACCESS_KEY);
        String bucket = this.getConfig(OSS_BUCKET);

        DefaultUriBuilderFactory df = new DefaultUriBuilderFactory();
        UriBuilder ub =  df.builder();
        ub.scheme("oss");
        ub.host(endpoint);
        ub.userInfo(accessId + ":" + accessKey);


        if (StringUtils.isBlank(schemaName)) {
            ub.pathSegment(bucket, dbName, tableName);
        } else {
            ub.pathSegment(bucket, dbName, schemaName, tableName);
        }

        return ub.build().toString();
    }
    @Override
    public List<String> itemMasks() {
        return Arrays.asList(new String[]{
                "source.database.whitelist",
                "source.database.blacklist",
        });
    }
}
