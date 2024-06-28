package com.aliyun.odps.mma.meta;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;

import java.util.List;
import java.util.Objects;

public interface MetaLoader {
    void checkConfig(SourceConfig config) throws Exception;

    List<String> listDatabaseNames() throws Exception;
    DataBaseModel getDatabase(String dbName) throws Exception;

    List<String> listTableNames(String dbName) throws Exception;
    default List<TableModel> listTables(String dbName) throws Exception {
        return null;
    }
    TableModel getTable(String dbName, String tableName) throws Exception;

    List<PartitionModel> listPartitions(String dbName, String tableName) throws Exception;
    default List<PartitionModel> listPartitions(String dbName, String schemaName, String tableName) throws Exception {
        if (Objects.isNull(schemaName)) {
            return listPartitions(dbName, tableName);
        }

        return null;
    }
    PartitionModel getPartition(String dbName, String tableName, List<String> partitionValues) throws Exception;

    void open(SourceConfig config) throws Exception;
    void close();
    SourceType sourceType();
}
