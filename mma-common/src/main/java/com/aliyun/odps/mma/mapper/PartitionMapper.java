package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.model.PartitionModel;

import com.aliyun.odps.mma.query.PtFilter;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public interface PartitionMapper {
    List<PartitionModel> getPartitionsOfTable(@Param("tableId") int tableId);
    int getPartitionsNumOfTable(@Param("tableId") int tableId);
    List<PartitionModel> getPartitionsOfTask(@Param("taskId") int taskId);
    List<Integer> getPartitionIdsOfTask(@Param("taskId") int taskId);
    List<PartitionModel> getPartitionsOfTableBasic(@Param("tableId") int tableId);
    List<PartitionModel> getPartitionsOfTablesBasic(@Param("tableIds") List<Integer> tableIds);
    PartitionModel getPartitionById(@Param("id") int id);
    void insertPartition(PartitionModel partitionModel);
    void updatePartition(PartitionModel partitionModel);
    List<PartitionModel> getPartitions(@Param("ids") List<Integer> ids);
    void updatePartitionsStatus(@Param("status") MigrationStatus status, @Param("taskId") Integer taskId);
    List<PartitionModel> getPartitionsBasicByDsId(
            @Param("sourceId") int sourceId,
            @Param("maxItem") int maxItem,
            @Param("marker") int marker);
    List<PartitionModel> getPartitionsOfDbsBasic(@Param("dbIds") List<Integer> tableIds);
    List<Map<String, Object>> ptStatOfDbs(@Param("dbIds") List<Integer> dbIds);
    List<Map<String, Object>> ptStatOfTables(@Param("tableIds") List<Integer> tableIds);
    List<Map<String, Object>> ptStat();
    void setPartitionsStatusInitByJobId(@Param("jobId") int jobId);
    void setTerminatedPtStatusInit();
    List<PartitionModel> getPts(PtFilter ptFilter);
    int getPtsCount(PtFilter ptFilter);
    int resetPtStatus(@Param("ptIds") List<Integer> ptIds);
}
