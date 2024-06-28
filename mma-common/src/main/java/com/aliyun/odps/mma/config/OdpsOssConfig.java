package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.OdpsPartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;
import com.aliyun.odps.mma.util.StringUtils;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilder;

public class OdpsOssConfig extends SourceConfig {
    @ConfigItem(desc = "maxcompute endpoint", required = true)
    public static String MC_ENDPOINT = "mc.endpoint";
    @ConfigItem(desc = "maxcompute access id", required = true)
    public static String MC_AUTH_ACCESS_ID = "mc.auth.access.id";
    @ConfigItem(desc = "maxcompute access key", required = true, type = "password")
    public static String MC_AUTH_ACCESS_KEY = "mc.auth.access.key";
    public static String MC_DEFAULT_PROJECT = "mc.default.project";
    @ConfigItem(desc = "要迁移的maxcompute projects", type = "list", required = true)
    public static String MC_PROJECTS = "mc.projects";
    @ConfigItem(desc = "maxcompute endpoint http connect timeout(单位s)", type = "int", defaultValue = "3")
    public static String MC_REST_CONN_TIMEOUT = "mc.rest.conn.timeout";
    @ConfigItem(desc = "maxcompute endpoint http read timeout(单位s)", type = "int", defaultValue = "5")
    public static String MC_REST_READ_TIMEOUT = "mc.rest.read.timeout";
    @ConfigItem(desc = "maxcompute endpoint http try times", type = "int", defaultValue = "1")
    public static String MC_REST_TRY_TIMES = "mc.rest.try.times";
    @ConfigItem(desc = "maxcompute 迁移任务sql参数", type = "map", defaultValue = "{\n    " +
            "\"odps.sql.hive.compatible\": \"true\"," +
            "\"odps.sql.split.hive.bridge\": \"true\"" +
            "\n}")
    public static String MC_SQL_HINTS = "mc.sql.hints";
    @ConfigItem(desc = "oss endpoint for src maxcompute project", required = true)
    public static final String OSS_ENDPOINT_SRC = "oss.endpoint.src";
    @ConfigItem(desc = "oss endpoint for dst maxcompute project", required = true)
    public static final String OSS_ENDPOINT_DST = "oss.endpoint.dst";
    @ConfigItem(desc = "oss bucket", required = true)
    public static final String OSS_BUCKET = "oss.bucket";
    @ConfigItem(desc = "oss access id", required = true)
    public static final String OSS_AUTH_ACCESS_ID = "oss.auth.access.id";
    @ConfigItem(desc = "oss access key", required = true, type = "password")
    public static final String OSS_AUTH_ACCESS_KEY = "oss.auth.access.key";

    public OdpsOssConfig() {
        super();
    }

    public String getOssPathOfSrcExternalTable(String dbName, String schemaName, String tableName) {
        return getOssPathOfExternalTable(
                getConfig(OSS_ENDPOINT_SRC),
                dbName,
                schemaName,
                tableName
        );
    }

    public String getOssPathOfDstExternalTable(String dbName, String schemaName, String tableName) {
        return getOssPathOfExternalTable(
                getConfig(OSS_ENDPOINT_DST),
                dbName,
                schemaName,
                tableName
        );
    }

    private String getOssPathOfExternalTable(String endpoint, String dbName, String schemaName, String tableName) {
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
    public PartitionGrouping getPartitionGrouping() {
        return new OdpsPartitionGrouping();
    }

    @Override
    public TaskType defaultTaskType() {
        return TaskType.ODPS_OSS_TRANSFER;
    }

    @Override
    public SourceType getSourceType() {
        return null;
    }
}
