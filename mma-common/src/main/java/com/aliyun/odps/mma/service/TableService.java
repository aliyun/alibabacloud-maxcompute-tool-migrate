package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.query.TableFilter;
import com.aliyun.odps.mma.util.TableHasher;

import java.util.List;
import java.util.Map;

public interface TableService {
    List<TableModel> getTablesOfDb(int dbId);
    List<TableModel> getTablesOfDataSource(String sourceName);
    List<TableModel> getTablesOfDbWithWhiteList(int dbId, List<String> whiteList);
    List<TableModel> getTablesOfDbWithBlackList(int dbId, List<String> blackList);
    TableModel getTableById(int id);
    void insertTable(TableModel tm);
    void updateTable(TableModel tm);
    void batchInsertTables(List<TableModel> tmList);
    void batchUpdateTables(List<TableModel> tmList);
    List<TableHasher> getAllTableHasher();
    List<TableModel> getTablesOfDbByNames(String dsName, String dbName, List<String> tableNames);
    List<TableModel> getAllTables();
    List<TableModel> getTables(TableFilter tableFilter);
    int getTablesCount(TableFilter tableFilter);
    List<TableModel> getTablesOfDbs(List<Integer> dbIds);
    void updateTableStatus(int tableId, MigrationStatus status);
    List<Map<String, Object>> tableStatOfDbs(List<Integer> dbIds);
    List<String> getTableNamesByDbId(int dbId);
}
