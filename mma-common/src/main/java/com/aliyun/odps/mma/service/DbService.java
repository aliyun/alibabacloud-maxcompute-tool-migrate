package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.query.DbFilter;

import java.util.List;
import java.util.Optional;

public interface DbService {
    List<DataBaseModel> getAllDbs();
    List<DataBaseModel> getDbsOfDataSource(String sourceName);
    Optional<DataBaseModel> getDbById(int id);
    Optional<DataBaseModel> getDbByName(String dsName, String dbName);
    void insertDb(DataBaseModel db);
    void updateDb(DataBaseModel db);
    void batchUpdateDb(List<DataBaseModel> dbList);
    List<DataBaseModel> getDbs(DbFilter dbFilter);
    int getDbsCount(DbFilter dbFilter);
}
