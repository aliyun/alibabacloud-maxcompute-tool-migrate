package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.mapper.DataSourceMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.query.PtFilter;
import com.aliyun.odps.mma.service.PartitionService;

import com.aliyun.odps.mma.util.MysqlConfig;
import com.aliyun.odps.mma.util.StepIter;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.*;

@Service
public class PartitionServiceImpl implements PartitionService {
    private static final Logger logger = LoggerFactory.getLogger(PartitionServiceImpl.class);
    private SqlSessionFactory sqlSessionFactory;
    private PartitionMapper mapper;
    private DataSourceMapper dsMapper;
    private MysqlConfig mysqlConfig;
    private DataSource dbDataSource;

    @Autowired
    public PartitionServiceImpl(
            SqlSessionFactory sqlSessionFactory,
            PartitionMapper mapper,
            DataSourceMapper dsMapper,
            DataSource dbDataSource,
            MysqlConfig mysqlConfig
    ) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.mapper = mapper;
        this.dsMapper = dsMapper;
        this.dbDataSource = dbDataSource;
        this.mysqlConfig = mysqlConfig;
    }

    @Override
    public List<PartitionModel> getPartitions(List<Integer> partitionIds) {
        if (Objects.isNull(partitionIds) || partitionIds.isEmpty()) {
            return Collections.emptyList();
        }

        return this.mapper.getPartitions(partitionIds);
    }

    @Override
    public List<PartitionModel> getPartitionsOfTable(int tableId) {
        return this.mapper.getPartitionsOfTable(tableId);
    }

    @Override
    public int getPartitionsNumOfTable(int tableId) {
        return this.mapper.getPartitionsNumOfTable(tableId);
    }

    @Override
    public List<PartitionModel> getPartitionsOfTask(int taskId) {
        return this.mapper.getPartitionsOfTask(taskId);
    }

    @Override
    public List<Integer> getPartitionIdsOfTask(int taskId) {
        return this.mapper.getPartitionIdsOfTask(taskId);
    }

    @Override
    public List<PartitionModel> getPartitionsOfTableBasic(int tableId) {
        return this.mapper.getPartitionsOfTableBasic(tableId);
    }

    @Override
    public List<PartitionModel> getPartitionsOfTablesBasic(List<Integer> tableIds) {
        StepIter<Integer> tableStepIter = new StepIter<>(tableIds, 10000);

        List<PartitionModel> partitions = new ArrayList<>();
        for (List<Integer> tableSubList: tableStepIter) {
            List<PartitionModel> ptSubList = this.mapper.getPartitionsOfTablesBasic(tableSubList);
            if (Objects.nonNull(ptSubList)) {
                partitions.addAll(ptSubList);
            }
        }

        return partitions;
    }

    @Override
    public PartitionModel getPartitionById(int id) {
        return this.mapper.getPartitionById(id);
    }

    @Override
    public void insertPartition(PartitionModel partitionModel) {
        this.mapper.insertPartition(partitionModel);
    }

    @Override
    public void batchInsertPartitions(List<PartitionModel> pmList) {
        int maxBatch = mysqlConfig.getMaxBatchSize();
        StepIter<PartitionModel> tableStepIter = new StepIter<>(pmList, maxBatch);

        for (List<PartitionModel> subList: tableStepIter) {
            _batchInsertPartitions(subList);
        }
    }

    public void _batchInsertPartitions(List<PartitionModel> pmList) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            PartitionMapper mapper = sqlSession.getMapper(PartitionMapper.class);

            for (PartitionModel pm: pmList) {
                mapper.insertPartition(pm);
            }

            sqlSession.commit();
        }


    }

    @Override
    public void batchUpdatePartitions(List<PartitionModel> pmList) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH)) {
            PartitionMapper mapper = sqlSession.getMapper(PartitionMapper.class);

            for (PartitionModel pm: pmList) {
                mapper.updatePartition(pm);
            }

            sqlSession.commit();
        }

//        try (Connection conn = dbDataSource.getConnection()) {
//            String sql = "insert into partition_model\n" +
//                    " (source_id, db_id, table_id, db_name, schema_name, table_name, value,\n" +
//                    "   status,\n" +
//                    "   size, num_rows, last_ddl_time, create_time\n" +
//                    " )\n" +
//                    " values\n" +
//                    " (\n" +
//                    "   ?, ?, ?, ?, ?, ?, ?,\n" +
//                    "    ?,\n" +
//                    "    ?, ?, ?, now()\n" +
//                    " )";

//            PreparedStatement ps = conn.prepareStatement(sql);
//
//            for (PartitionModel pm: pmList) {
//                ps.setInt(1, pm.getSourceId());
//                ps.setInt(2, pm.getDbId());
//                ps.setInt(3, pm.getTableId());
//                ps.setString(4, pm.getDbName());
//                ps.setString(5, pm.getSchemaName());
//                ps.setString(6, pm.getTableName());
//                ps.setString(7, pm.getValue());
//
//                ps.setString(8, pm.getStatus().name());
//
//                ps.setLong(9, pm.getSize());
//                ps.setLong(10, pm.getNumRows());
//
//                if (Objects.nonNull(pm.getLastDdlTime())) {
//                    ps.setDate(11, new Date(pm.getLastDdlTime().getTime()));
//                } else {
//                    ps.setDate(11, null);
//                }
//
//                ps.addBatch();
//            }
//
//            ps.executeBatch();
//            conn.commit();
//        } catch (SQLException e) {
//            logger.error("failed to save partitions", e);
//            throw new RuntimeException(e);
//        }
    }

    @Override
    public List<PartitionModel> getAllPartitions() {
        List<PartitionModel> partitions = this.mapper.getPartitions(Collections.emptyList());

        if (Objects.nonNull(partitions)) {
            return partitions;
        }

        return Collections.emptyList();
    }

    @Override
    public List<PartitionModel> getPartitionsOfDataSource(String sourceName) {
        DataSourceModel ds = dsMapper.getDataSourceByName(sourceName);
        if (Objects.isNull(ds)) {
            return new ArrayList<>();
        }

        // 分页获取partition
        int maxItem = 10000;
        int marker = 0;

        List<PartitionModel> partitions = new ArrayList<>();

        while (true) {
            List<PartitionModel> page = this.mapper.getPartitionsBasicByDsId(ds.getId(), maxItem, marker);

            if (page.isEmpty()) {
                break;
            }

            partitions.addAll(page);
            logger.info("get {} partitions", partitions.size());
            marker = page.get(page.size() - 1).getId();
        }

        return partitions;
    }

    @Override
    public void updatePartitionsStatus(MigrationStatus status, Integer taskId) {
        mapper.updatePartitionsStatus(status, taskId);
    }

    @Override
    public List<PartitionModel> getPartitionsOfDbsBasic(List<Integer> dbIds) {
        return mapper.getPartitionsOfDbsBasic(dbIds);
    }

    @Override
    public List<Map<String, Object>> ptStatOfDbs(List<Integer> dbIds) {
        if (dbIds.isEmpty()) {
            return new ArrayList<>();
        }
        return mapper.ptStatOfDbs(dbIds);
    }

    @Override
    public List<Map<String, Object>> ptStatOfTables(List<Integer> tableIds) {
        if (tableIds.isEmpty()) {
            return new ArrayList<>();
        }
        return mapper.ptStatOfTables(tableIds);
    }

    @Override
    public List<PartitionModel> getPts(PtFilter ptFilter) {
        return mapper.getPts(ptFilter);
    }

    @Override
    public int getPtsCount(PtFilter ptFilter) {
        return mapper.getPtsCount(ptFilter);
    }

    @Override
    public int resetPtStatus(List<Integer> ptIds) {
        if (ptIds.isEmpty()) {
            return 0;
        }
        return mapper.resetPtStatus(ptIds);
    }
}
