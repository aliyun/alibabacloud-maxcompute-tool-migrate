package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.mapper.DataSourceMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.query.PtFilter;
import com.aliyun.odps.mma.service.PartitionService;

import com.aliyun.odps.mma.util.StepIter;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class PartitionServiceImpl implements PartitionService {
    SqlSessionFactory sqlSessionFactory;
    PartitionMapper mapper;
    DataSourceMapper dsMapper;

    @Autowired
    public PartitionServiceImpl(SqlSessionFactory sqlSessionFactory, PartitionMapper mapper, DataSourceMapper dsMapper) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.mapper = mapper;
        this.dsMapper = dsMapper;
    }

    @Override
    public List<PartitionModel> getPartitions(List<Integer> partitionIds) {
        if (Objects.isNull(partitionIds) || partitionIds.size() == 0) {
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
    @Transactional
    public void batchInsertPartitions(List<PartitionModel> pmList) {
        int maxBatch = 50*10000;
        StepIter<PartitionModel> tableStepIter = new StepIter<>(pmList, maxBatch);

        for (List<PartitionModel> subList: tableStepIter) {
            _batchInsertPartitions(subList);
        }
    }

    @Transactional
    public void _batchInsertPartitions(List<PartitionModel> pmList) {
        try (SqlSession sqlSession = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            PartitionMapper mapper = sqlSession.getMapper(PartitionMapper.class);

            for (PartitionModel pm: pmList) {
                mapper.insertPartition(pm);
            }

            sqlSession.commit();
        }

        // batch提交的时候，mybatis只能获取最后一个id，把这个id设置给pmList的第一个元素。
        // 所以，所有元素的id需要自己计算出来
//        int n = pmList.size();
//        if (n > 0) {
//            PartitionModel firstPm = pmList.get(0);
//            int lastId = firstPm.getId();
//            int offset = lastId - n + 1;
//
//            for (int i = n - 1; i >= 0; i --) {
//                PartitionModel pm = pmList.get(i);
//                pm.setId(offset + i);
//            }
//        }
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

        return this.mapper.getPartitionsByDsId(ds.getId());
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
        if (dbIds.size() == 0) {
            return new ArrayList<>();
        }
        return mapper.ptStatOfDbs(dbIds);
    }

    @Override
    public List<Map<String, Object>> ptStatOfTables(List<Integer> tableIds) {
        if (tableIds.size() == 0) {
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
        if (ptIds.size() == 0) {
            return 0;
        }
        return mapper.resetPtStatus(ptIds);
    }
}
