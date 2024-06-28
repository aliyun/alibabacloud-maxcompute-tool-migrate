package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.DatabricksPartitionGrouping;
import com.aliyun.odps.mma.task.PartitionGrouping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatabricksConfig extends SourceConfig {
    @ConfigItem(desc = "Workspace URL", required = true)
    public static final String DB_WORKSPACE_URL = "db.workspace.url";
    @ConfigItem(desc = "Id of cluster with databricks runtime version of Spark 3.3.2", required = true)
    public static final String DB_CLUSTER_ID = "db.cluster.id";
    @ConfigItem(desc = "Access Token", required = true)
    public static final String DB_ACCESS_TOKEN = "db.access.token";
    @ConfigItem(desc = "DBFS root", required = true, defaultValue = "/FileStore/jars/")
    public static final String DB_DBFS_ROOT = "db.dbfs.root";
    @ConfigItem(desc = "Notebook Job Name", required = true, defaultValue = "data_migration")
    public static final String DB_NOTEBOOK_JOB_NAME = "db.notebook.job.name";
    @ConfigItem(desc = "Notebook github url", required = true, defaultValue = "https://github.com/fetchadd/mma_databricks.git")
    public static final String DB_NOTEBOOK_GITHUB_URL = "db.notebook.github.url";
    @ConfigItem(desc = "Notebook github path", required = true, defaultValue = "notebooks/mma_task")
    public static final String DB_NOTEBOOK_GITHUB_PATH = "db.notebook.github.path";
    @ConfigItem(desc = "Notebook github branch", required = true, defaultValue = "main")
    public static final String DB_NOTEBOOK_GITHUB_BRANCH = "db.notebook.github.main";
    @ConfigItem(desc = "Notebook job id", required = false, editable = false)
    public static final String DB_NOTEBOOK_JOB_ID = "db.notebook.job.id";
    @ConfigItem(desc = "单个任务处理的最多分区数量", type = "int", defaultValue = "50")
    public static final String DB_TASK_PARTITION_MAX_NUM = "db.task.partition.max.num";
    @ConfigItem(desc = "单个任务处理的最大数量(单位G)", type = "int", defaultValue = "5")
    public static final String DB_TASK_PARTITION_MAX_SIZE = "db.task.partition.max.size";

    @ConfigItem(desc = "表黑名单, 格式为db.table", type="list", defaultValue = "[" +
            "\"*.catalog_privileges\",\"*.catalog_tags\",\"*.catalogs\",\"*.check_constraints\"," +
            "\"*.column_masks\",\"*.column_tags\",\"*.columns\",\"*.constraint_column_usage\"," +
            "\"*.constraint_table_usage\",\"*.information_schema_catalog_name\",\"*.key_column_usage\"," +
            "\"*.parameters\",\"*.referential_constraints\",\"*.routine_columns\",\"*.routine_privileges\"," +
            "\"*.routines\",\"*.row_filters\",\"*.schema_privileges\",\"*.schema_tags\",\"*.schemata\"," +
            "\"*.table_constraints\",\"*.table_privileges\",\"*.table_tags\",\"*.tables\",\"*.views\"," +
            "\"*.volume_privileges\",\"*.volume_tags\",\"*.volumes\",\"*.catalog_provider_share_usage\"," +
            "\"*.connections\",\"*.external_location_privileges\",\"*.external_locations\"," +
            "\"*.metastore_privileges\",\"*.metastores\",\"*.providers\",\"*.recipient_allowed_ip_ranges\"," +
            "\"*.recipient_tokens\",\"*.recipients\",\"*.share_recipient_privileges\",\"*.shares\"," +
            "\"*.storage_credential_privileges\",\"*.storage_credentials\",\"*.table_share_usage\"]"
    )
    public static String TABLE_BLACKLIST = "source.table.blacklist";

    public String getWorkspaceUrl() {
        return getConfig(DB_WORKSPACE_URL);
    }

    public String getClusterId() {
        return getConfig(DB_CLUSTER_ID);
    }

    public String getJdbcUrl() {
        Pattern urlPattern = Pattern.compile(
                "https://(?<host>adb-(?<hostId>\\d+)\\.(?:\\d+)\\.(?:\\w+)\\.(?:net|com))"
        );
        String workspaceURL = getConfig(DB_WORKSPACE_URL);
        Matcher matcher =  urlPattern.matcher(workspaceURL);

        if (!matcher.matches()) {
            throw new RuntimeException("invalid workspace url: " + workspaceURL);
        }

        String host = matcher.group("host");
        String hostId = matcher.group("hostId");
        String clusterId = getConfig(DB_CLUSTER_ID);

        return String.format(
                "jdbc:databricks://%s:443/default;transportMode=http;ssl=1;httpPath=sql/protocolv1/o/%s/%s;AuthMech=3;UID=token;",
                host,
                hostId,
                clusterId
        );
    }

    public String getNotebookJobName() {
        return getConfig(DB_NOTEBOOK_JOB_NAME);
    }

    public String getGithubURL() {
        return getConfig(DB_NOTEBOOK_GITHUB_URL);
    }

    public String getGithubBranch() {
        return getConfig(DB_NOTEBOOK_GITHUB_BRANCH);
    }

    public String getGithubPath() {
        return getConfig(DB_NOTEBOOK_GITHUB_PATH);
    }

    public String getAccessToken() {
        return getConfig(DB_ACCESS_TOKEN);
    }

    public String getDbfsRoot() {
        return getConfig(DB_DBFS_ROOT);
    }

    public Long getNotebookJobId() {
        return getLong(DB_NOTEBOOK_JOB_ID);
    }

    public void setNotebookJobId(Long jobId) {
        setConfig(DB_NOTEBOOK_JOB_ID, jobId.toString());
    }

    @Override
    public PartitionGrouping getPartitionGrouping() {
        return new DatabricksPartitionGrouping(
                getInteger(DB_TASK_PARTITION_MAX_NUM),
                getInteger(DB_TASK_PARTITION_MAX_SIZE)
        );
    }

    @Override
    public TaskType defaultTaskType() {
        return TaskType.DATABRICKS;
    }

    @Override
    public SourceType getSourceType() {
        return SourceType.DATABRICKS;
    }
}
