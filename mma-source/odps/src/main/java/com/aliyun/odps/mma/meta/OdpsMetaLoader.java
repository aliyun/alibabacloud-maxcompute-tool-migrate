package com.aliyun.odps.mma.meta;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.execption.MMAException;
import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class OdpsMetaLoader implements MetaLoader {
    private OdpsConfig config;
    private Odps odps;
    private ExecutorService threadPool;
    private Gson gson = new Gson();

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
        RestClient restClient = this.odps.getRestClient();
        restClient.setConnectTimeout(this.config.getInteger(OdpsConfig.MC_REST_CONN_TIMEOUT));
        restClient.setReadTimeout(this.config.getInteger(OdpsConfig.MC_REST_READ_TIMEOUT));
        restClient.setRetryTimes(this.config.getInteger(OdpsConfig.MC_REST_TRY_TIMES));
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
        Iterator<Table> tableIter = this.odps.tables().iterator(dbName);
        List<String> tables = new ArrayList<>();
        tableIter.forEachRemaining(t -> tables.add(t.getName()));
        return tables;
    }

    @Override
    public TableModel getTable(String dbName, String tableName) throws Exception {
        Table table = this.odps.tables().get(dbName, tableName);
        TableModel tm = new TableModel();
        tm.setDbName(dbName);
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

        Map<String, Object> extra = new HashMap<>();
        extra.put("CryptoAlgo", table.getCryptoAlgoName());
        extra.put("StorageHandler", table.getStorageHandler());
        Map<String, String> serDeProperties = table.getSerDeProperties();
        if (Objects.nonNull(serDeProperties) && serDeProperties.size() > 0) {
            extra.put("SerDeProperties", serDeProperties);
        }

        Gson gson = new Gson();
        String extraJson = gson.toJson(extra);
        tm.setExtra(extraJson);

        return tm;
    }

    @Override
    public List<PartitionModel> listPartitions(String dbName, String tableName) throws Exception {
        Table table = this.odps.tables().get(dbName, tableName);
        Iterator<Partition> ptIter = table.getPartitionIterator(null);
        List<PartitionModel> pmList = new LinkedList<>();
        while (ptIter.hasNext()) {
            Partition partition = ptIter.next();

            PartitionModel pm = new PartitionModel();
            pm.setDbName(dbName);
            pm.setTableName(tableName);
            pm.setLastDdlTime(partition.getLastDataModifiedTime());

            PartitionSpec ps = partition.getPartitionSpec();
            pm.setValue(ps.toString(false, true));
            //pm.setSize(partition.getSize()); 这里会触发一次http请求，分区很多的情况下严重拖慢获取元数据的速度

            pmList.add(pm);
        }

        return pmList;
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
}
