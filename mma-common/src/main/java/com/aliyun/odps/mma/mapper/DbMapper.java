package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.query.DbFilter;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DbMapper {
    List<DataBaseModel> getAllDbs();
    List<DataBaseModel> getDbsOfDataSource(@Param("sourceName") String sourceName);
    DataBaseModel getDbById(@Param("id") int id);
    DataBaseModel getDbByName(@Param("dsName") String dsName, @Param("dbName") String dbName);
    void insertDb(DataBaseModel db);
    void updateDb(DataBaseModel db);
    void batchUpdateDbsStatus(@Param("status") MigrationStatus status, @Param("ids") List<Integer> ids);
    List<DataBaseModel> getDbs(DbFilter dbFilter);
    int getDbsCount(DbFilter dbFilter);
}
