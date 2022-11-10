package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.query.TableFilter;
import com.aliyun.odps.mma.util.TableHasher;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public interface TableMapper {
    List<TableModel> getTablesOfDb(@Param("dbId") int dbId);
    List<MigrationStatus> getTablesStatusOfDb(@Param("dbId") int dbId);
    List<TableModel> getTablesOfDbWithWhiteList(@Param("dbId") int dbId, @Param("whiteList") List<String> whiteList);
    List<TableModel> getTablesOfDbWithBlackList(@Param("dbId") int dbId, @Param("blackList") List<String> blackList);
    TableModel getTableById(@Param("id") int id);
    void insertTable(TableModel tm);
    void updateTable(TableModel tm);
    List<TableHasher> getAllTableHasher();
    List<TableModel> getTablesOfDbByNames(@Param("dsName") String dsName, @Param("dbName") String dbName, @Param("tables") List<String> tableNames);
    void updateTablesStatus(@Param("status") MigrationStatus status, @Param("ids") List<Integer> ids);
    List<TableModel> getAllTables();
    List<TableModel> getTablesOfDataSource(@Param("sourceName")  String sourceName);
    List<TableModel> getTables(TableFilter tableFilter);
    int getTablesCount(TableFilter tableFilter);
    List<TableModel> getTablesOfDbs(@Param("dbIds") List<Integer> dbIds);
    void updateTableStatus(@Param("tableId") int tableId, @Param("status") MigrationStatus status);
    void setNonPartitionedTableStatusInitByJobId(@Param("jobId") int jobId);
    void batchUpdateTableStatus(@Param("status") MigrationStatus status, @Param("tableIds") List<Integer> tableIds);
    List<Map<String, Object>> tableStat();
    List<Map<String, Object>> tableStatOfDbs(@Param("dbIds") List<Integer> dbIds);
    List<String> getTableNamesByDbId(@Param("dbId") int dbId);
}
