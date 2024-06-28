package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.HiveGluePartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilder;

import java.util.*;

public class HiveGlueConfig extends HiveConfig {
    @ConfigItem(desc = "aws region", required = true)
    public static final String AWS_REGION = "aws.region";
    @ConfigItem(desc = "aws access id", required = true)
    public static final String AWS_ACCESS_ID = "aws.access.id";
    @ConfigItem(desc = "aws access key", required = true, type = "password")
    public static final String AWS_ACCESS_KEY = "aws.access.key";
    @ConfigItem(desc = "oss endpoint internal, use in mc sql", required = true)
    public static final String OSS_ENDPOINT_INTERNAL = "oss.endpoint.internal";
    @ConfigItem(desc = "oss endpoint external, use in mma if mma can not use internal endpoint", required = true)
    public static final String OSS_ENDPOINT_EXTERNAL = "oss.endpoint.external";
    @ConfigItem(desc = "oss access id", required = true)
    public static final String OSS_AUTH_ACCESS_ID = "oss.auth.access.id";
    @ConfigItem(desc = "oss access key", required = true, type = "password")
    public static final String OSS_AUTH_ACCESS_KEY = "oss.auth.access.key";
    @ConfigItem(desc = "maxcompute 迁移任务sql参数", type = "map", defaultValue = "{\n    " +
            "\"odps.sql.hive.compatible\": \"true\"," +
            "\"odps.sql.split.hive.bridge\": \"true\"" +
            "\n}")
    public static String MC_SQL_HINTS = "mc.sql.hints";

    public HiveGlueConfig() {
        super();
    }

    /**
     * 根据hive table 的s3文件路径，获取oss路径
     */
    public String getTableDataPathFromS3(String s3Path) {
        //s3路径示例 s3://datahousedev01/dw_dim/test
        String path = s3Path.substring("s3://".length());

        String endpoint = this.getConfig(OSS_ENDPOINT_INTERNAL);
        String accessId = this.getConfig(OSS_AUTH_ACCESS_ID);
        String accessKey = this.getConfig(OSS_AUTH_ACCESS_KEY);

        DefaultUriBuilderFactory df = new DefaultUriBuilderFactory();
        UriBuilder ub =  df.builder();
        ub.scheme("oss");
        ub.host(endpoint);
        ub.userInfo(accessId + ":" + accessKey);
        ub.path(path);

        return ub.build().toString();
    }

    @Override
    public PartitionGrouping getPartitionGrouping() {
        return new HiveGluePartitionGrouping(
            this.getInteger(HIVE_TASK_PARTITION_MAX_NUM),
            this.getInteger(HIVE_TASK_PARTITION_MAX_SIZE)
        );
    }

    @Override
    public TaskType defaultTaskType() {
        return TaskType.HIVE_OSS;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.HIVE_GLUE;
    }

    @Override
    protected List<String> itemMasks() {
        List<String> masks = new ArrayList<>(super.itemMasks());
        masks.addAll(Arrays.asList(
                HiveConfig.HIVE_METASTORE_URLS,
                HiveConfig.HIVE_METASTORE_CLIENT_SOCKET_TIMEOUT
        ));
        return masks;
    }
}