package com.aliyun.odps.mma.meta;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.mapper.DbMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.mapper.TableMapper;
import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.model.*;
import com.aliyun.odps.mma.service.DataSourceService;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.service.PartitionService;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.util.TableHasher;
import lombok.Getter;
import lombok.Setter;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataSourceMetaLoader {
    Logger logger = LoggerFactory.getLogger(DataSourceMetaLoader.class);

    @Setter
    private MetaLoader loader;
    @Getter
    private List<DataBaseModel> dataBases;
    @Getter
    private List<TableModel> tables;
    @Getter
    private List<PartitionModel> partitions;

    private ExecutorService threadPool;
    //  进度信息，用整数来表示小数，保存小数点后两位。也就是progress=250时，进度是2.5%
    private final AtomicInteger progress = new AtomicInteger(0);
    private Map<String, DataBaseModel> dbNameToDb;
    private Map<String, Map<String, TableModel>> dbNameToTables;
    private SourceConfig config;
    private Integer dsId;
    private DataSourceModel dataSource;

    private final DataSourceService dsService;
    private final DbService dbService;
    private final TableService tableService;
    private final PartitionService ptService;
    private final SqlSessionFactory sqlSessionFactory;

    @Autowired
    public DataSourceMetaLoader(
            DataSourceService dsService,
            DbService dbService,
            TableService tableService,
            PartitionService ptService,
            SqlSessionFactory sqlSessionFactory) {
        this.dsService = dsService;
        this.dbService = dbService;
        this.tableService = tableService;
        this.ptService = ptService;
        this.sqlSessionFactory = sqlSessionFactory;
    }

    public void open(DataSourceModel dsModel) throws Exception {
        this.config = dsModel.getConfig();
        this.dsId = dsModel.getId();
        this.dataSource = dsModel;
        this.loader.open(this.config);
        this.threadPool = Executors.newFixedThreadPool(config.getMetaApiBatch());
    }

    public void close() {
        this.threadPool.shutdown();
        this.loader.close();
    }

    public void updateData() throws Exception {
        this.loadData();

        String sourceName = config.getSourceName();

        logger.info("start to get existed partitions");
        List<PartitionModel> oldPartitions = ptService.getPartitionsOfDataSource(sourceName);
        logger.info("success to get existed partitions, total {}", oldPartitions.size());
        List<TableModel> oldTables = tableService.getTablesOfDataSource(sourceName);
        List<DataBaseModel> oldDatabases = dbService.getDbsOfDataSource(sourceName);

        List<PartitionModel> partitionUpdated = new ArrayList<>();
        List<PartitionModel> partitionsNew = new ArrayList<>();

        List<TableModel> tablesUpdated = new ArrayList<>();
        List<TableModel> tablesNew = new ArrayList<>();

        List<DataBaseModel> dbsUpdated = new ArrayList<>();
        List<DataBaseModel> dbsNew = new ArrayList<>();

        CalUpdate<PartitionModel> calPt = new CalUpdate<>();
        CalUpdate<TableModel> calTable = new CalUpdate<>();
        CalUpdate<DataBaseModel> calDb = new CalUpdate<>();

        calPt.calUpdatedAndNew(this.partitions, oldPartitions, partitionUpdated, partitionsNew);
        calTable.calUpdatedAndNew(this.tables, oldTables, tablesUpdated, tablesNew);
        calDb.calUpdatedAndNew(this.dataBases, oldDatabases, dbsUpdated, dbsNew);

        for (TableModel table: tablesUpdated) {
            String dbName = table.getDbName();

            for (DataBaseModel db: oldDatabases) {
                if (dbName.equals(db.getName()) && !dbsUpdated.contains(db)) {
                    dbsUpdated.add(db);
                }
            }
        }

        // 保存db name到db的映射，在保存新的table的时能够获取table的db_id值
        oldDatabases.forEach(dm -> this.dbNameToDb.put(dm.getName(), dm));

        // 保存table name到table的映射，保存新的partition时能够获取partition的table_id, db_id值
        oldTables.forEach(tm -> {
            String dbName = tm.getDbName();
            String tbName = tm.getName();
            Map<String, TableModel> tables = dbNameToTables.get(dbName);
            tables.put(tbName, tm);
        });

        logger.info(
                "try update {} dbs, {} tables, {} partitions in database",
                dbsUpdated.size(), tablesUpdated.size(), partitionUpdated.size()
        );

        // 启事务，update, insert数据
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            PartitionMapper ptMapper = sqlSession.getMapper(PartitionMapper.class);
            TableMapper tableMapper = sqlSession.getMapper(TableMapper.class);
            DbMapper dbMapper = sqlSession.getMapper(DbMapper.class);

            for (PartitionModel p: partitionUpdated) {
                ptMapper.updatePartition(p);
            }

            for (TableModel t: tablesUpdated) {
                tableMapper.updateTable(t);
            }

            for (DataBaseModel d: dbsUpdated) {
                dbMapper.updateDb(d);
            }

            sqlSession.commit();
        }

        logger.info(
                "success to update {} dbs, {} tables, {} partitions in database",
                dbsUpdated.size(), tablesUpdated.size(), partitionUpdated.size()
        );

        // 保存新增的数据
        this.saveNewData(dbsNew, tablesNew, partitionsNew);
    }

    private void loadData() throws Exception {
        this.dataBases = new ArrayList<>();
        this.tables = new ArrayList<>();
        this.partitions = new LinkedList<>(); // partitions太多了, 所以用link lis

        this.loadAllDbs();
        this.dataSource.setDbNum(this.dataBases.size());
        progress.set(100);
        logger.info("loading meta {}%", getProgress());
        this.loadAllTables();
        this.dataSource.setTableNum(this.tables.size());
        progress.set(500);
        logger.info("loading meta {}%", getProgress());
        logger.info("start load partitions");
        this.loadAllPartitions();
        this.dataSource.setPartitionNum(this.partitions.size());
        logger.info("success to load partitions");
        logger.info("load meta data ok");

        this.dbNameToDb = new HashMap<>(this.dataBases.size());
        this.dbNameToTables = new HashMap<>(this.dataBases.size());

        this.dataBases.forEach(dm -> {
            dm.setSourceId(this.dsId);
            String dbName = dm.getName();
            dbNameToDb.put(dbName, dm);
            dbNameToTables.put(dbName, new HashMap<>());
        });

        // 累加table的size到db
        this.tables.forEach(tm -> {
            tm.setSourceId(this.dsId);
            String dbName = tm.getDbName();
            String tbName = tm.getName();
            Map<String, TableModel> tables = dbNameToTables.get(dbName);
            tables.put(tbName, tm);

            DataBaseModel dm = dbNameToDb.get(dbName);
            tm.setDbName(dm.getName());

            Optional<Long> sizeOpt = tm.getSizeOpt();
            sizeOpt.ifPresent(s -> dm.setSize(dm.getSizeOpt().orElse(0L) + s));

            Optional<Long> numRowsOpt = tm.getNumRowsOpt();
            numRowsOpt.ifPresent(n -> dm.setNumRows(dm.getNumRowsOpt().orElse(0L) + n));
        });

        // 累加partition的size, numRows到table, db, 如果是hive, 更新partition value值:v1/v2 -> k1=v1/k2=v2
        this.partitions.forEach(pm -> {
            pm.setSourceId(this.dsId);
            Long size = pm.getSizeOpt().orElse(0L);
            Long numRows = pm.getNumRowsOpt().orElse(0L);

            String dbName = pm.getDbName();
            String tableName = pm.getTableName();

            DataBaseModel dm = dbNameToDb.get(dbName);
            TableModel tm = dbNameToTables.get(dbName).get(tableName);

            pm.setDbName(dm.getName());
            pm.setTableName(tm.getName());

            dm.setSize(dm.getSizeOpt().orElse(0L) + size);
            tm.setSize(tm.getSizeOpt().orElse(0L) + size);

            dm.setNumRows(dm.getNumRowsOpt().orElse(0L) + numRows);
            tm.setNumRows(tm.getNumRowsOpt().orElse(0L) + numRows);

            MMATableSchema schema = tm.getSchema();
            if (tm.isHasPartitions()) {
                List<MMAColumnSchema> partitionColumns = schema.getPartitions();

                String pmValue = pm.getValue();
                // 将partition value写成p1=xxx/p2=xx的形式
                if (! pmValue.contains("=")) {
                    String[] values = pmValue.split("/");
                    assert partitionColumns.size() == values.length;
                    StringBuilder sb = new StringBuilder();

                    for (int i = 0, n = values.length;  i < n; i ++) {
                        sb.append(partitionColumns.get(i).getName()).append("=").append(values[i]);

                        if (i < n - 1) {
                            sb.append("/");
                        }
                    }

                    pm.setValue(sb.toString());
                }
            }
        });
    }

    private void loadAllDbs() throws Exception {
        // 获取所有的database名字
        List<String> dbNames = loader.listDatabaseNames();
        List<String> dbWhiteList = config.getDbWhiteList();
        List<String> dbBlackList = config.getDbBlackList();

        Stream<String> dbNamesStream = dbNames.stream();

        if (dbWhiteList.size() > 0) {
             dbNamesStream = dbNamesStream.filter(dbWhiteList::contains);
        }

        if (dbBlackList.size() > 0) {
            dbNamesStream = dbNamesStream.filter(n -> !dbBlackList.contains(n));
        }

        dbNames = dbNamesStream.collect(Collectors.toList());

        // 获取所有database具体信息
        List<Future<DataBaseModel>> futures = new ArrayList<>(dbNames.size());

        dbNames.forEach(dbName -> {
            Future<DataBaseModel> future = threadPool.submit(() -> loader.getDatabase(dbName));
            futures.add(future);
        });

        FuturesResult<DataBaseModel> fr = new FuturesResult<>(futures);
        fr.get(r -> dataBases.add(r));
    }

    private void loadAllTables() throws Exception {
        List<String> whiteList = config.getTableWhiteList();
        List<String> blackList = config.getTableBlackList();

        BiFunction<String, String, Boolean> tableFilter = (dbName, tableName) -> {
            String tableFullName = String.format("%s.%s", dbName, tableName);
            if (whiteList.size() > 0) {
                return whiteList.contains(tableFullName);
            }

            return ! blackList.contains(tableFullName);
        };

        for(DataBaseModel db: dataBases) {
            // 获取一个db的所有table名字
            String dbName = db.getName();
            List<String> tableNames = this.loader.listTableNames(dbName);

            // 获取table的具体信息
            int tbNum = tableNames.size();
            List<Future<TableModel>> futures = new ArrayList<>(tbNum);

            for(String tbName: tableNames) {
                if (! tableFilter.apply(dbName, tbName)) {
                    continue;
                }

                Future<TableModel> future = threadPool.submit(() -> loader.getTable(dbName, tbName));
                futures.add(future);
            }

            FuturesResult<TableModel> fr = new FuturesResult<>(futures);
            fr.get(r -> tables.add(r));
        }
    }

    private void loadAllPartitions() throws Exception {
        List<Future<List<PartitionModel>>> futures = new ArrayList<>(this.tables.size());

        this.tables.forEach(table -> {
            if (! table.isHasPartitions()) {
                return;
            }

            Future<List<PartitionModel>> future = threadPool.submit(
                    () -> this.loader.listPartitions(table.getDbName(), table.getName())
            );

            futures.add(future);
        });

        FuturesResult<List<PartitionModel>> fr = new FuturesResult<>(futures);
        int n = futures.size();
        // 这一步，进度最多推进到70%
        int adder = (int)((70.0 / n) * 100);

        fr.get(r -> {
            this.partitions.addAll(r);
            progress.addAndGet(adder);
            logger.info("loading meta  {}%", getProgress());
        });
    }

    private static class FuturesResult<T> {
        Logger logger = LoggerFactory.getLogger(FuturesResult.class);

        List<Future<T>> futures;

        public FuturesResult(List<Future<T>> futures) {
            this.futures = futures;
        }

        public void get(Consumer<T> c) throws Exception {
            for (Future<T> f: futures) {
                T result = f.get();
                c.accept(result);
            }
//            futures.forEach(f -> {
//                try {
//                    T result = f.get();
//                    c.accept(result);
//                } catch (InterruptedException | ExecutionException e) {
//                    logger.error("", e);
//                }
//            });
        }
    }

    private static class CalUpdate<T extends ModelBase> {
        Logger logger = LoggerFactory.getLogger(CalUpdate.class);

        private void calUpdatedAndNew(
                List<T> newModels, List<T> oldModels,
                List<T> modelsUpdated, List<T> modelsNew
        ) {
            Map<T, T> oldModelsMap = new HashMap<>(oldModels.size());
            oldModels.forEach(m -> oldModelsMap.put(m, m));

            for (T newModel: newModels) {
                T oldModel = oldModelsMap.get(newModel);

                if (Objects.isNull(oldModel)) {
                    modelsNew.add(newModel);
                    continue;
                }

                boolean sizeUpdated = ! Objects.equals(newModel.getSize(), oldModel.getSize());
                boolean numRowsUpdated = !Objects.equals(newModel.getNumRows(), oldModel.getNumRows());
                boolean lastDdlTimeUpdated = !Objects.equals(newModel.getLastDdlTime(), oldModel.getLastDdlTime());

                if (sizeUpdated) {
                    log("size of {} is updated", oldModel);
                }

                if (numRowsUpdated) {
                    log("num rows of {} is updated", oldModel);
                }

                if (lastDdlTimeUpdated) {
                    log("last ddl time of {} is updated", oldModel);
                }

                boolean isUpdated = sizeUpdated || numRowsUpdated || lastDdlTimeUpdated;

                if (oldModel instanceof TableModel) {
                    if (!Objects.equals(((TableModel) newModel).getSchema(), ((TableModel) oldModel).getSchema())) {
                        isUpdated = true;
                        log("table schema of {} is updated", oldModel);
                    }
                }

                if (isUpdated) {
                    oldModel.setSize(newModel.getSize());
                    oldModel.setNumRows(newModel.getNumRows());
                    oldModel.setLastDdlTime(newModel.getLastDdlTime());

                    if (oldModel instanceof TableModel) {
                        ((TableModel) oldModel).setSchema(((TableModel) newModel).getSchema());
                    }

                    modelsUpdated.add(oldModel);
                }
            }
        }

        private void log(String tpl, ModelBase model) {
            String modelId = "";
            if (model instanceof DataBaseModel) {
                modelId = String.format("db %s", ((DataBaseModel) model).getName());
            } else if (model instanceof TableModel) {
                TableModel tm = (TableModel) model;
                modelId = String.format("table %s.%s", tm.getDbName(), tm.getName());
            } else if (model instanceof PartitionModel) {
                PartitionModel pm = (PartitionModel) model;
                modelId = String.format("partition %s.%s.%s", pm.getDbName(), pm.getTableName(), pm.getValue());
            }

            logger.info(tpl, modelId);
        }
    }

    private void saveNewData(List<DataBaseModel> dataBases, List<TableModel> tables, List<PartitionModel> partitions) {
        // 保存db
        dataBases.forEach(this.dbService::insertDb);

        logger.info("save {} new dbs to database", dataBases.size());

        // 填充table model的db_id字段
        tables.forEach(tm -> {
            DataBaseModel dm = dbNameToDb.get(tm.getDbName());
            tm.setDbId(dm.getId());
        });

        //  批量保存table, 这时table里的id字段不能获取
        tableService.batchInsertTables(tables);

        // 获取table的ids
        List<TableHasher> tableHasherList = tableService.getAllTableHasher();
        Map<TableHasher, Integer> tableIdMap = new HashMap<>();
        tableHasherList.forEach(tableHasher -> {
            Integer tableId = tableHasher.getTableId();
            tableHasher.setTableId(null);
            tableIdMap.put(tableHasher, tableId);
        });

        tables.forEach(tm -> {
            tm.setId(null);
            TableHasher hasher = tm.getTableHasher();
            Integer tableId = tableIdMap.get(hasher);
            tm.setId(tableId);
        });

        progress.set(7500);
        logger.info("loading meta {}%", getProgress());
        logger.info("save {} new tables to database", tables.size());

        logger.info("start to save {} new partitions to database", partitions.size());

        // 填充partition model的db_id, table_id字段
        partitions.forEach(pm -> {
            String dbName = pm.getDbName();
            String tableName = pm.getTableName();

            DataBaseModel dm = dbNameToDb.get(dbName);
            TableModel tm = dbNameToTables.get(dbName).get(tableName);

            pm.setDbId(dm.getId());
            pm.setTableId(tm.getId());
        });

        // 批量把partitions插入db
        ptService.batchInsertPartitions(partitions);
        logger.info("save {} new partitions to database", partitions.size());

        // 更新数据源统计信息
        dsService.updateDataSource(dataSource);

        progress.set(10000);
        logger.info("loading meta {}%", getProgress());
    }

    public float getProgress() {
        return progress.get() / (float)100.0;
    }
}
