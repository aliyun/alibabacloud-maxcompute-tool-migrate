package com.aliyun.odps.mma.meta;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.ModelBase;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.util.DateUtils;

@Component
@Primary
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveMetaLoader implements MetaLoader {
    private HiveConfig config;
    private IMetaStoreClient[] clients;
    private Semaphore semaphore;
    private ConcurrentMap<Integer, Boolean> occupation;
    private int clientNum = -1;

    @Override
    public void open(SourceConfig sourceConfig) throws Exception {
        assert sourceConfig instanceof HiveConfig;
        this.config = (HiveConfig) sourceConfig;

        int batch = config.getMetaApiBatch();
        this.clientNum = batch;
        this.clients = new IMetaStoreClient[batch];
        this.semaphore = new Semaphore(batch);
        this.occupation = new ConcurrentHashMap<>(batch);

        if (clientNum == 1) {
            this.clients[0] = createClient(this.config);
            return;
        }

        ExecutorService pool = Executors.newFixedThreadPool(clientNum);
        Future<Integer>[] futures = new Future[clientNum];

        for (int i = 0; i < clientNum; i ++) {
            int j = i;

            futures[i] = pool.submit(() -> {
                this.clients[j] = createClient(this.config);
                return 0;
            });
        }

        for (Future<Integer> future: futures) {
            future.get();
        }

        pool.shutdown();
    }

    @Override
    public void close() {
        for (int i = 0; i < clientNum; i ++) {
            if (Objects.nonNull(clients[i])) {
                clients[i].close();
            }
        }
    }

    @Override
    public SourceType sourceType() {
        return SourceType.HIVE;
    }

    @Override
    public void checkConfig(SourceConfig config) throws Exception {
        createClient(config);
    }

    @Override
    public List<String> listDatabaseNames() throws Exception {
        try (Client client = this.getClient()){
            return client.instance().getAllDatabases();
        }
    }

    @Override
    public DataBaseModel getDatabase(String dbName) throws Exception {
        try (Client client = this.getClient()) {
            Database db = client.instance().getDatabase(dbName);

            DataBaseModel model = new DataBaseModel();
            model.setName(db.getName());
            model.setOwner(db.getOwnerName());
            model.setDescription(db.getDescription());
            model.setLocation(db.getLocationUri());

            return model;
        }
    }

    @Override
    public List<String> listTableNames(String dbName) throws Exception {
        try (Client client = this.getClient()) {
            return client.instance().listTableNamesByFilter(dbName, "", (short) -1);
        }
    }

    @Override
    public TableModel getTable(String dbName, String tableName) throws Exception {
        Table table = null;

        try (Client client = this.getClient()) {
            table = client.instance().getTable(dbName, tableName);
        }

        TableModel model = new TableModel();
        model.setDbName(dbName);
        model.setName(tableName);
        model.setOwner(table.getOwner());
        model.setType(table.getTableType());

        StorageDescriptor sd = table.getSd();
        if (Objects.nonNull(sd)) {
            model.setLocation(sd.getLocation());
            model.setInputFormat(sd.getInputFormat());
            model.setOutputFormat(sd.getOutputFormat());

            SerDeInfo serDeInfo = sd.getSerdeInfo();

            if (Objects.nonNull(serDeInfo)) {
                model.setSerde(serDeInfo.getSerializationLib());
                Map<String, String> serdeProperties = serDeInfo.getParameters();

                if (Objects.nonNull(serdeProperties)) {
                    model.setExtraMap(serdeProperties);
                }
            }
        }

        Map<String, String> params =  table.getParameters();
        setModelParam(params, model);

        List<FieldSchema> cols = table.getSd().getCols();
        List<FieldSchema> partitionCols = table.getPartitionKeys();

        MMATableSchema ts = new MMATableSchema(tableName);
        ts.setColumns(cols.stream().map(HiveMMASchemaAdapter::fieldSchemaToColumn).collect(Collectors.toList()));
        ts.setPartitions(partitionCols.stream().map(HiveMMASchemaAdapter::fieldSchemaToColumn).collect(Collectors.toList()));
        model.setSchema(ts);
        model.setHasPartitions(!partitionCols.isEmpty());

        return model;
    }

    @Override
    public List<PartitionModel> listPartitions(String dbName, String tableName) throws Exception {
        List<Partition> partitions = null;

        try (Client client = this.getClient()) {
            partitions =  client.instance().listPartitions(dbName, tableName, (short) -1);
        }

        return partitions.stream().map(this::convertPartition).collect(Collectors.toList());
    }

    @Override
    public PartitionModel getPartition(String dbName, String tableName, List<String> partitionValues) throws Exception {
        try (Client client = this.getClient()) {
            Partition partition = client.instance().getPartition(dbName, tableName, partitionValues);
            return convertPartition(partition);
        }
    }

    private PartitionModel convertPartition(Partition partition) {
        PartitionModel model = new PartitionModel();

        model.setDbName(partition.getDbName());
        model.setTableName(partition.getTableName());
        model.setValue(partition.getValues());

        Map<String, String> params = partition.getParameters();
        setModelParam(params, model);

        return model;
    }

    private static void setModelParam(Map<String, String> params, ModelBase model) {
        if (Objects.nonNull(params)) {
            String rawDataSizeStr = params.get("rawDataSize");
            String numRowsStr = params.get("numRows");
            String lastDdlTimeStr = params.get("transient_lastDdlTime");

            if (Objects.nonNull(rawDataSizeStr)) {
                model.setSize(Long.parseLong(rawDataSizeStr));
            }

            if (Objects.nonNull(numRowsStr)) {
                model.setNumRows(Long.parseLong(numRowsStr));
            }

            if (Objects.nonNull(lastDdlTimeStr)) {
                int lastDdlTime = Integer.parseInt(lastDdlTimeStr);
                model.setLastDdlTime(DateUtils.fromSeconds(lastDdlTime));
            }
        }
    }

    private static class Client implements AutoCloseable {
        private final HiveMetaLoader parent;
        private final int index;

        public Client(HiveMetaLoader parent, int index) {
            this.parent = parent;
            this.index = index;
        }

        public IMetaStoreClient instance() {
            return parent.clients[this.index];
        }

        @Override
        public void close() throws Exception {
            this.parent.occupation.remove(this.index);
            this.parent.semaphore.release();
        }
    }

    private Client getClient() throws Exception {
        semaphore.acquire();

        for (int i = 0; i < clientNum; i ++) {
            synchronized (this) {
                if (occupation.containsKey(i)) {
                    continue;
                }

                occupation.put(i, true);
                return new Client(this, i);
            }
        }

        throw new RuntimeException("unreachable");
    }

    private IMetaStoreClient createClient(SourceConfig config) throws Exception {
        Configuration conf = new Configuration();
        HiveConf hiveConf = new HiveConf(conf, Configuration.class);

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, config.getOrErr(HiveConfig.HIVE_METASTORE_URLS));
        hiveConf.setVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, config.getConfig(HiveConfig.HIVE_METASTORE_CLIENT_SOCKET_TIMEOUT));

        if (config.getBoolean(HiveConfig.HIVE_METASTORE_SASL_ENABLED)) {
            String keyTableFile = config.getOrErr(HiveConfig.HIVE_METASTORE_KERBEROS_KEYTAB_FILE);
            String principal = config.getOrErr(HiveConfig.HIVE_METASTORE_KERBEROS_PRINCIPAL);

            hiveConf.setVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, "true");
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE, keyTableFile);
            hiveConf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);

            String gssJaasFile = config.getOrErr(HiveConfig.JAVA_SECURITY_AUTH_LOGIN_CONFIG);
            String krb5File = config.getOrErr(HiveConfig.JAVA_SECURITY_KRB5_CONF);

            System.setProperty("java.security.auth.login.config", gssJaasFile);
            System.setProperty("java.security.krb5.conf", krb5File);
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        }

        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, _x -> null, HiveMetaStoreClient.class.getName());
        client.getAllDatabases();
        return client;
    }
}
