package com.aliyun.odps.mma.meta;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.execption.MMAException;
import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import com.aliyun.odps.mma.meta.schema.MMATableConstraint;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.task.ClusterInfo;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.util.MMAFlag;
import com.aliyun.odps.mma.util.StringUtils;
import com.aliyun.odps.rest.RestClient;
import com.aliyun.odps.task.SQLTask;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OdpsMetaLoader implements MetaLoader {
    private Logger logger = LoggerFactory.getLogger(OdpsMetaLoader.class);
    private OdpsConfig config;
    private Odps odps;
    private Gson gson = new Gson();
    private boolean migrateStorageTier = false;
    private boolean migrateLifeCycleStatus = false;
    private final Pattern nameWithSchemaPtn = Pattern.compile("(?<schema>[^\\.]+)\\.(?<name>[^\\.]+)");

    @Override
    public void open(SourceConfig config) throws Exception {
        assert config instanceof OdpsConfig;
        this.config = (OdpsConfig) config;

        String accessId = this.config.getOrErr(OdpsConfig.MC_AUTH_ACCESS_ID);
        String accessKey = this.config.getOrErr(OdpsConfig.MC_AUTH_ACCESS_KEY);
        String endpoint = this.config.getOrErr(OdpsConfig.MC_ENDPOINT);
        String defaultProject = this.config.getOrErr(OdpsConfig.MC_DEFAULT_PROJECT);

        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        this.odps = new Odps(account);
        this.odps.setEndpoint(endpoint);
        this.odps.setDefaultProject(defaultProject);
        String mmaFlag = MMAFlag.getMMAFlag(this.odps);
        this.odps.setUserAgent(mmaFlag);

        RestClient restClient = this.odps.getRestClient();
        restClient.setConnectTimeout(this.config.getInteger(OdpsConfig.MC_REST_CONN_TIMEOUT));
        restClient.setReadTimeout(this.config.getInteger(OdpsConfig.MC_REST_READ_TIMEOUT));
        restClient.setRetryTimes(this.config.getInteger(OdpsConfig.MC_REST_TRY_TIMES));

        this.migrateStorageTier = this.config.getBoolean(OdpsConfig.MC_MIGRATE_STORAGE_TIER);
        this.migrateLifeCycleStatus = this.config.getBoolean(OdpsConfig.MC_MIGRATE_LIFECYCLE_STATUS);
    }

    @Override
    public void close() {

    }

    @Override
    public void checkConfig(SourceConfig config) throws Exception {
        open(config);

        boolean ok = this.odps.projects().exists(this.odps.getDefaultProject());
        if (! ok) {
            throw new MMAException(String.format("project %s is not existed", this.odps.getDefaultProject()));
        }
    }

    @Override
    public List<String> listDatabaseNames() throws Exception {
        return config.getProjects();
    }

    @Override
    public DataBaseModel getDatabase(String dbName) throws Exception {
        Project project = this.odps.projects().get(dbName);
        DataBaseModel dm = new DataBaseModel();

        dm.setName(project.getName());
        dm.setDescription(project.getComment());
        dm.setOwner(project.getOwner());
        dm.setLastDdlTime(project.getLastModifiedTime());
        Map<String, String> property = project.getProperties();
        dm.setExtra(gson.toJson(property));

        return dm;
    }

    @Override
    public List<String> listTableNames(String dbName) throws Exception {
        Project odpsProject = odps.projects().get(dbName);

        String schemaEnabled = odpsProject.getProperty("odps.schema.model.enabled");

        List<String> tables = new ArrayList<>();
        if (!Objects.equals(schemaEnabled, "true")) {
            Iterator<Table> tableIter = this.odps.tables().iterator(dbName);

            tableIter.forEachRemaining(t -> {
                if (!t.getName().startsWith("mma_src_external_temp_")) {
                    tables.add(t.getName());
                }
            });
            return tables;
        }


        Iterator<Schema> schemaIter = this.odps.schemas().iterator(dbName);

        schemaIter.forEachRemaining(schema -> {
            odps.tables().iterable(dbName, schema.getName(), null, false).forEach(t -> {
                if (!t.getName().startsWith("mma_src_external_temp_")) {
                    tables.add(schema.getName() + "." + t.getName());
                }
            });
        });

        return tables;
    }

    @Override
    public TableModel getTable(String dbName, String _tableName) throws Exception {
        try {
            return _getTable(dbName, _tableName);
        } catch (Exception e) {
            logger.error("failed to get table {}.{}", dbName, _tableName, e);
            throw e;
        }
    }

    public TableModel _getTable(String dbName, String _tableName) throws Exception {
        Matcher matcher = nameWithSchemaPtn.matcher(_tableName);

        String tableName;
        Table table;

        if (matcher.matches()) {
            String schemaName = matcher.group("schema");
            tableName = matcher.group("name");
            table = this.odps.tables().get(dbName, schemaName, tableName);
        } else {
            tableName = _tableName;
            table = this.odps.tables().get(dbName, tableName);
        }

        TableModel tm = new TableModel();
        table.reload();
        tm.setDbName(dbName);
        tm.setSchemaName(table.getSchemaName());
        tm.setName(tableName);

        if (table.isExternalTable()) {
            tm.setType("EXTERNAL_TABLE");
        } else if (table.isVirtualView()) {
            tm.setType("VIRTUAL_VIEW");
        } else {
            tm.setType("MANAGED_TABLE");
        }

        TableSchema odpsSchema = table.getSchema();
        MMATableSchema mmaSchema = new MMATableSchema(tableName);
        mmaSchema.setComment(table.getComment());

        Function<Column, MMAColumnSchema> columnConvert = (Column c) -> {
            return new MMAColumnSchema(
                    c.getName(),
                    c.getTypeInfo().getTypeName(),
                    c.getComment(),
                    c.getDefaultValue(),
                    c.isNullable()
            );
        };

        List<MMAColumnSchema> mmaColumns = odpsSchema
                .getColumns()
                .stream()
                .map(columnConvert)
                .collect(Collectors.toList());
        List<MMAColumnSchema> mmaPtColumns = odpsSchema
                .getPartitionColumns()
                .stream()
                .map(columnConvert)
                .collect(Collectors.toList());

        mmaSchema.setColumns(mmaColumns);
        mmaSchema.setPartitions(mmaPtColumns);
        tm.setSchema(mmaSchema);
        tm.setHasPartitions(mmaPtColumns.size() > 0);
        tm.setOwner(table.getOwner());
        tm.setSize(table.getSize());
        tm.setLastDdlTime(table.getLastDataModifiedTime());
        if (table.getRecordNum() > 0) {
            tm.setNumRows(table.getRecordNum());
        }

        if (table.isTransactional()) {
            mmaSchema.setEnableTransaction(true);
            if (ListUtils.size(table.getPrimaryKey()) > 0) {
                mmaSchema.setTableConstraints(Collections.singletonList(new MMATableConstraint(table.getPrimaryKey())));
            }
        }

        Table.ClusterInfo odpsClusterInfo = table.getClusterInfo();
        if (odpsClusterInfo != null) {
            ClusterInfo.ClusterType clusterType = ClusterInfo.ClusterType.valueOf(odpsClusterInfo.getClusterType().toLowerCase());
            ClusterInfo info = new ClusterInfo(odpsClusterInfo.getClusterCols(),
                    odpsClusterInfo.getBucketNum(),
                    clusterType,
                    odpsClusterInfo.getSortCols());
            mmaSchema.setClusterInfo(info);
        }

        Map<String, Object> extra = new HashMap<>();
        extra.put("CryptoAlgo", table.getCryptoAlgoName());
        extra.put("StorageHandler", table.getStorageHandler());
        Map<String, String> serDeProperties = table.getSerDeProperties();
        if (Objects.nonNull(serDeProperties) && serDeProperties.size() > 0) {
            extra.put("SerDeProperties", serDeProperties);
        }

        // 直接设置分层存储，只有非分区表能设置，分区表需要设置到分区
        Map<String, String> tblProperties = new HashMap<>();
        if (migrateStorageTier) {
            if (!table.isPartitioned()) {
                StorageTierInfo storageTierInfo = table.getStorageTierInfo();
                if (storageTierInfo != null) {
                    tblProperties.put("storageTierInfo", storageTierInfo.getStorageTier().getName());
                }
            }
        }

        String reserved = table.getReserved();


        if (StringUtils.isNotBlank(reserved)) {

            JsonObject reservedJson;
            try {
                reservedJson = new JsonParser().parse(reserved).getAsJsonObject();
            } catch (Exception e) {
                throw new IllegalArgumentException("table reserved is invalid, table is " + dbName + "." + tableName + " reserved: " + reserved);
            }

            if (migrateStorageTier) {
                // 通过生命周期设置分层存储，只能在项目 or 分区表级别设置
                // 项目级别的配置由用户自己保证
                String lifecycleConfig = getLifecycleConfig(reservedJson);
                if (StringUtils.isNotBlank(lifecycleConfig)) {
                    tblProperties.put("lifecycleConfig", lifecycleConfig);
                }
            }

            if (disableLifeCycle(reservedJson)) {
                tm.getSchema().setDisableLifeCycle(true);
            }

//            if (migrateLifeCycleStatus) {
//                if (disableLifeCycle(reservedJson)) {
//                    tblProperties.put("DisableLifeCycle", "true");
//                }
//            }
        }


        if (Objects.nonNull(table.getType()) && !"VIRTUAL_VIEW".equalsIgnoreCase(table.getType().name())) {
            String ddl = getShowCreateTableResult(table);
            // 如果 lifecycle status = disabled, odps worker 返回 -1，无法获取准确的 lifecycle，只能通过 show create table sql 获取
            if (tm.getSchema().isDisableLifeCycle()) {
                int ddlLifeCycle = getLifeCycle(ddl);
                if (ddlLifeCycle > 0) {
                    tm.setLifecycle(ddlLifeCycle);
                }
            } else {
                long lifecycle = table.getLife();
                if (lifecycle > 0) {
                    tm.setLifecycle((int) lifecycle);
                }
            }

            Set<String> reservedKeys = new HashSet<>(Arrays.asList(
                    "write.bucket.num",
                    "acid.cdc.mode.enable",
                    "acid.data.retain.hours",
                    "cdc.data.retain.hours",
                    "cdc.insert.into.passthrough.enable"
            ));

            for (Map.Entry<String, String> entry : extractTblProperties(ddl).entrySet()) {
                if (reservedKeys.contains(entry.getKey())) {
                    tblProperties.put(entry.getKey(), entry.getValue());
                }
            }
        }

        mmaSchema.setTblProperties(tblProperties);

        Gson gson = new Gson();
        String extraJson = gson.toJson(extra);
        tm.setExtra(extraJson);

        return tm;
    }

    @Override
    public List<PartitionModel> listPartitions(String dbName, String schemaName, String tableName) throws Exception {
        Table table;

        if (Objects.isNull(schemaName)) {
            table = this.odps.tables().get(dbName, tableName);
        } else {
            table = this.odps.tables().get(dbName, schemaName, tableName);
        }

        Iterator<Partition> ptIter = table.getPartitionIterator(null);
        List<PartitionModel> pmList = new LinkedList<>();
        while (ptIter.hasNext()) {
            Partition partition = ptIter.next();

            PartitionModel pm = new PartitionModel();
            pm.setDbName(dbName);
            if (Objects.nonNull(schemaName)) {
                pm.setSchemaName(schemaName);
            }
            pm.setTableName(tableName);
            pm.setLastDdlTime(partition.getLastDataModifiedTime());

            PartitionSpec ps = partition.getPartitionSpec();
            pm.setValue(ps.toString(false, true));
            //pm.setSize(partition.getSize()); 这里会触发一次http请求，分区很多的情况下严重拖慢获取元数据的速度

            if (migrateStorageTier || migrateLifeCycleStatus) {
                Map<String, String> extraInfo = new HashMap<>();

                if (migrateStorageTier) {
                    // 优先使用分区上的存储类型，保证低频迁移后，不会因为 lifecycle 配置变成高频
                    if (partition.getStorageTierInfo() != null) {
                        extraInfo.put("storageTierInfo", partition.getStorageTierInfo().getStorageTier().getName());
                    }
                }

                String reserved = null;

                try {
                    reserved = partition.getReserved();
                } catch (Exception e) {
                    // 这个地方要严格判断一下报错，如果是404，说明分区在更新的过程中被删除了
                    continue;
                }

                if (StringUtils.isNotBlank(reserved)) {
                    JsonObject reservedJson;
                    try {
                        reservedJson = new JsonParser().parse(reserved).getAsJsonObject();
                    } catch (Exception e) {
                        throw new IllegalArgumentException("partition reserved is invalid, partition is "
                                + dbName + "." + tableName + "/" + pm.getValue() + " reserved: " + reserved);
                    }

                    if (migrateLifeCycleStatus && disableLifeCycle(reservedJson)) {
                        extraInfo.put("disableLifeCycle", "true");
                    }

                }
                pm.setExtra(gson.toJson(extraInfo));
            }

            pmList.add(pm);
        }

        return pmList;
    }

    @Override
    public List<PartitionModel> listPartitions(String dbName, String tableName) throws Exception {
        return null;
    }

    @Override
    public PartitionModel getPartition(String dbName, String tableName, List<String> partitionValues) throws Exception {
        // TODO: remove this method
        return null;
    }


    @Override
    public SourceType sourceType() {
        return SourceType.ODPS;
    }

    private String getLifecycleConfig(JsonObject reservedJson) {
        if (reservedJson.has("LifecycleConfig")) {
            return reservedJson.get("LifecycleConfig").getAsString();
        }
        return null;
    }

    private boolean disableLifeCycle(JsonObject reservedJson) {
        if (reservedJson.has("LifecycleState")) {
            if ("disabled".equalsIgnoreCase(reservedJson.get("LifecycleState").getAsString())) {
                return true;
            }
        }
        return false;
    }

    private static Pattern pattern = Pattern.compile("(?i).*?\\bLIFECYCLE\\s+(\\d+)\\s*;?\\s*$");

    private static int getLifeCycle(String ddl) {
        Matcher matcher = pattern.matcher(ddl.trim()); // 先去除首尾空格
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    private static final Pattern TBLPROPERTIES_PATTERN = Pattern.compile("(?i)TBLPROPERTIES\\s*\\(([^)]+)\\)");
    private static final Pattern KV_PATTERN = Pattern.compile("'(?<key>[^']+)'\\s*=\\s*'(?<value>[^']*)'");

    private String getShowCreateTableResult(Table table) throws OdpsException {
        String tableFullName = table.getProject() + "." + table.getName();
        if (!StringUtils.isBlank(table.getSchemaName())) {
            tableFullName = table.getProject() + "." + table.getSchemaName() + "." + table.getName();
        }
        Instance instance = SQLTask.run(odps, "show create table " + tableFullName + ";");
        instance.waitForSuccess();
        return instance.getRawTaskResults().get(0).getResult().getString();
    }

    public static Map<String, String> extractTblProperties(String ddl) {
        boolean[] inQuotes = computeQuoteStatus(ddl);
        Matcher matcher = TBLPROPERTIES_PATTERN.matcher(ddl);

        // 存储所有可能的 TBLPROPERTIES 匹配区间
        List<int[]> matches = new ArrayList<>();
        while (matcher.find()) {
            matches.add(new int[]{matcher.start(), matcher.end(), matcher.group(1).length()});
        }

        // 从后往前找第一个不在引号内的 TBLPROPERTIES
        for (int i = matches.size() - 1; i >= 0; i--) {
            int start = matches.get(i)[0];
            if (!isInsideQuotes(start, inQuotes)) {
                return parseProperties(ddl.substring(matches.get(i)[0], matches.get(i)[1]));
            }
        }
        return new HashMap<>();
    }

    // 判断某个位置是否在引号内
    private static boolean isInsideQuotes(int pos, boolean[] inQuotes) {
        return pos < inQuotes.length && inQuotes[pos];
    }

    // 预处理字符串，标记每个字符是否在引号内
    private static boolean[] computeQuoteStatus(String str) {
        boolean[] status = new boolean[str.length()];
        boolean inQuote = false, escaped = false;

        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (escaped) {
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '\'') {
                inQuote = !inQuote;
            }
            status[i] = inQuote;
        }
        return status;
    }

    // 解析 TBLPROPERTIES 内容
    private static Map<String, String> parseProperties(String segment) {
        Map<String, String> map = new HashMap<>();
        Matcher m = KV_PATTERN.matcher(segment);
        while (m.find()) {
            map.put(m.group("key"), m.group("value"));
        }
        return map;
    }

    public static void main(String[] args) {
        System.out.println(extractTblProperties("CREATE TABLE IF NOT EXISTS odps_test_tunnel_project_gcc492.wuyue0103(s STRING) STORED AS ALIORC TBLPROPERTIES ('transactional'='true','a'='\\'b');"));
        System.out.println(extractTblProperties("CREATE TABLE IF NOT EXISTS odps_test_tunnel_project_gcc492.wuyue0103(s STRING COMMENT 'abc\\' tblproperties(\\'transactional\\'=\\'false\\')') STORED AS ALIORC TBLPROPERTIES ('transactional'='true');"));
        System.out.println(getLifeCycle("...lifecycle 7;"));
    }
}
