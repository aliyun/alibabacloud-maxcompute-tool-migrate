package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.query.PtFilter;

import java.util.List;
import java.util.Map;

public interface PartitionService {
    List<PartitionModel> getPartitions(List<Integer> partitionIds);
    List<PartitionModel> getPartitionsOfTable(int tableId);
    int getPartitionsNumOfTable(int tableId);
    List<PartitionModel> getPartitionsOfTask(int taskId);
    List<Integer> getPartitionIdsOfTask(int taskId);
    List<PartitionModel> getPartitionsOfTableBasic(int tableId);
    List<PartitionModel> getPartitionsOfTablesBasic(List<Integer> tableIds);
    PartitionModel getPartitionById(int id);
    void insertPartition(PartitionModel partitionModel);
    void batchInsertPartitions(List<PartitionModel> pmList);
    void batchUpdatePartitions(List<PartitionModel> pmList);
    List<PartitionModel> getAllPartitions();
    List<PartitionModel> getPartitionsOfDataSource(String sourceName);
    void updatePartitionsStatus(MigrationStatus status, Integer taskId);
    List<PartitionModel> getPartitionsOfDbsBasic(List<Integer> dbIds);
    List<Map<String, Object>> ptStatOfDbs(List<Integer> dbIds);
    List<Map<String, Object>> ptStatOfTables(List<Integer> tableIds);
    List<PartitionModel> getPts(PtFilter ptFilter);
    int getPtsCount(PtFilter ptFilter);
    int resetPtStatus(List<Integer> ptIds);
}
