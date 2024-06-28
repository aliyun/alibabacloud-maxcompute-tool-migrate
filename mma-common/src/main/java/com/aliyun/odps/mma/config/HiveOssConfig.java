package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.util.StringUtils;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.HivePartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriBuilder;

import java.util.HashMap;
import java.util.Map;

public class HiveOssConfig extends HiveConfig {
    @ConfigItem(desc = "oss endpoint internal, use in mc sql", required = true)
    public static final String OSS_ENDPOINT_INTERNAL = "oss.endpoint.internal";
    @ConfigItem(desc = "oss endpoint external, use in mma if mma can not use internal endpoint", required = true)
    public static final String OSS_ENDPOINT_EXTERNAL = "oss.endpoint.external";
    @ConfigItem(desc = "oss access id", required = true)
    public static final String OSS_AUTH_ACCESS_ID = "oss.auth.access.id";
    @ConfigItem(desc = "oss access key", required = true, type = "password")
    public static final String OSS_AUTH_ACCESS_KEY = "oss.auth.access.key";
    @ConfigItem(desc = "oss bucket", required = true)
    public static final String OSS_BUCKET = "oss.bucket";
    @ConfigItem(desc = "oss路径，去除bucket和db名字部分, a/b/")
    public static final String OSS_PATH_DB_DIR = "oss.path.db.dir";
    @ConfigItem(desc = "oss path db template, like \"${db}.db\"", defaultValue = "${db}")
    public static final String OSS_PATH_DB_TPL = "oss.path.db.tpl";
    @ConfigItem(desc = "maxcompute 迁移任务sql参数", type = "map", defaultValue = "{\n    " +
            "\"odps.sql.hive.compatible\": \"true\"," +
            "\"odps.sql.split.hive.bridge\": \"true\"" +
            "\n}")
    public static String MC_SQL_HINTS = "mc.sql.hints";

    public static String DB_PLACEHOLDER = "${db}";
    private static final Map<String, String> inputFormatToFileType = new HashMap<>();
    static {
        inputFormatToFileType.put("OrcInputFormat", "ORC");
        inputFormatToFileType.put("MapredParquetInputFormat", "PARQUET");
        inputFormatToFileType.put("SequenceFileInputFormat", "SEQUENCEFILE");
        inputFormatToFileType.put("TextInputFormat", "TEXTFILE");
        inputFormatToFileType.put("RCFileInputFormat", "RCFILE");
    }

    public HiveOssConfig() {
        super();
    }

    @Override
    public PartitionGrouping getPartitionGrouping() {
        return new HivePartitionGrouping(
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
        return SourceType.HIVE_OSS;
    }

    // OSS data path in sql:
    // oss://ak:sk@endpoint/bucket/prefix/DATA/db/table
    //
    // OSS metadata path:
    // bucket
    // │── metadata
    // │── dbName1
    // │   ├── FUNCTION
    // │   ├── RESOURCE
    // │   └── TABLE
    // │        └── tableName
    // │             ├── meta.json               # save table_model
    // │             ├── pt_meta.json.0          # save partition_model
    // │             └── pt_meta.json.1          # save partition_model
    // └── dbName2

    public String getTableDataPath(String db, String table) {
        String endpoint = this.getConfig(OSS_ENDPOINT_INTERNAL);
        String accessId = this.getConfig(OSS_AUTH_ACCESS_ID);
        String accessKey = this.getConfig(OSS_AUTH_ACCESS_KEY);
        String bucket = this.getConfig(OSS_BUCKET);
        String dbDir = this.getOrDefault(HiveOssConfig.OSS_PATH_DB_DIR, "");

        DefaultUriBuilderFactory df = new DefaultUriBuilderFactory();
        UriBuilder ub =  df.builder();
        ub.scheme("oss");
        ub.host(endpoint);
        ub.userInfo(accessId + ":" + accessKey);
        String dbPathPart = this.renderDbTpl(db);

        String[] paths = new String[]{
                bucket,
                StringUtils.trim(dbDir, "/"),
                StringUtils.trim(dbPathPart, "/"),
                table
        };

        String path = String.join( "/", paths);
        ub.path(path);

        return ub.build().toString();
    }

    public String getOdpsFileFormatByInputFormat(String inputFormat) {
        if (StringUtils.isBlank(inputFormat)) {
            throw new IllegalArgumentException("input format is empty & default storage type is not assigned.");
        }

        // inputFormat example: org.apache.hadoop.mapred.TextInputFormat
        String[] tmp = inputFormat.trim().split("\\.");
        inputFormat = tmp[tmp.length - 1];

        if (inputFormatToFileType.containsKey(inputFormat)) {
            return inputFormatToFileType.get(inputFormat);
        } else {
            throw new IllegalArgumentException("inputFormat not supported: " + inputFormat);
        }
    }


    private String renderDbTpl( String dbName) {
        String dbTpl = this.getOrDefault(OSS_PATH_DB_TPL, "");

        int start =  dbTpl.indexOf(DB_PLACEHOLDER);
        if (start < 0) {
            return dbTpl;
        }

        int offset = start + HiveOssConfig.DB_PLACEHOLDER.length();

        return dbTpl.substring(0, start) + dbName + dbTpl.substring(offset);
    }
}
