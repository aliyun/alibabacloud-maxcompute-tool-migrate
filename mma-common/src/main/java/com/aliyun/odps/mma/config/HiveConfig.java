package com.aliyun.odps.mma.config;

import java.util.Arrays;
import java.util.List;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.HivePartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;

//@Component
//@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveConfig extends SourceConfig {
    @ConfigItem(desc = "hive metastore url", required = true)
    public static final String HIVE_METASTORE_URLS = "hive.metastore.urls";
    @ConfigItem(desc = "hive metastore client socket timeout", type = "int", defaultValue = "600")
    public static final String HIVE_METASTORE_CLIENT_SOCKET_TIMEOUT = "hive.metastore.client.socket.timeout";
    @ConfigItem(desc = "hive jdbc url", required = true)
    public static final String HIVE_JDBC_URL = "hive.jdbc.url";
    @ConfigItem(desc = "hive jdbc user name", required = true)
    public static final String HIVE_JDBC_USERNAME = "hive.jdbc.username";
    @ConfigItem(desc = "hive jdbc password", defaultValue = "")
    public static final String HIVE_JDBC_PASSWORD = "hive.jdbc.password";
    @ConfigItem(desc = "hive metastore是否开启了kerberos认证", type = "boolean", defaultValue = "false")
    public static final String HIVE_METASTORE_SASL_ENABLED = "hive.metastore.sasl.enabled";
    @ConfigItem(desc = "kerberos principal")
    public static final String HIVE_METASTORE_KERBEROS_PRINCIPAL = "hive.metastore.kerberos.principal";
    @ConfigItem(desc = "kerberos keytab文件位置")
    public static final String HIVE_METASTORE_KERBEROS_KEYTAB_FILE = "hive.metastore.kerberos.keytab.file";
    @ConfigItem(desc = "kerberos gss-jaas.conf文件位置")
    public static final String JAVA_SECURITY_AUTH_LOGIN_CONFIG = "java.security.auth.login.config";
    @ConfigItem(desc = "kerberos krb5.conf文件位置")
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    @ConfigItem(desc = "单个任务处理的最多分区数量", type = "int", defaultValue = "50")
    public static final String HIVE_TASK_PARTITION_MAX_NUM = "hive.task.partition.max.num";
    @ConfigItem(desc = "单个任务处理的最大数量(单位G)", type = "int", defaultValue = "5")
    public static final String HIVE_TASK_PARTITION_MAX_SIZE = "hive.task.partition.max.size";
    @ConfigItem(desc = "hive job配置, 用于mr, spark, tez等引擎", type = "map", defaultValue = "{\n" +
            "  \"mapreduce.map.speculative\": \"false\",\n" +
            "  \"mapreduce.map.memory.mb\": \"8192\",\n" +
            "  \"yarn.scheduler.maximum-allocation-mb\": \"8192\",\n" +
            "  \"hive.fetch.task.conversion\": \"none\",\n" +
            "  \"mapreduce.task.timeout\": \"3600000\",\n" +
            "  \"mapreduce.job.running.map.limit\": \"100\",\n" +
            "  \"mapreduce.max.split.size\": \"512000000\",\n" +
            "  \"mapreduce.map.maxattempts\": \"0\",\n" +
            "  \"hive.execution.engine\": \"mr\"\n" +
            "}")
    public static final String HIVE_MR_JOB_SETTINGS = "hive.job.settings";
    @ConfigItem(desc = "数据库白名单", type="list")
    public static String DATABASE_WHITELIST = "source.database.whitelist";
    @ConfigItem(desc = "数据库黑名单", type="list", defaultValue = "[\"default\"]")
    public static String DATABASE_BLACKLIST = "source.database.blacklist";
    @ConfigItem(desc = "Hive UDTF 下载链接", defaultValue = "https://mma-v3.oss-cn-zhangjiakou.aliyuncs.com/udtf/hive-udtf.jar")
    public static String HIVE_UDTF_JAR_OSS_URL = "hive.udtf.jar.oss.url";
    @ConfigItem(desc = "Hive UDTF 名字", defaultValue = "default.odps_data_dump_multi")
    public static String HIVE_UDTF_NAME = "hive.udtf.name";
    @ConfigItem(desc = "Hive UDTF Class", defaultValue = "hive.com.aliyun.odps.mma.io.McDataTransmissionUDTF")
    public static String HIVE_UDTF_CLASS = "hive.udtf.class";


    public HiveConfig() {
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
        return TaskType.HIVE;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.HIVE;
    }

    @Override
    protected List<String> itemMasks() {
        return Arrays.asList(
            HiveConfig.HIVE_UDTF_NAME,
            HiveConfig.HIVE_UDTF_CLASS
        );
    }
}
