package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.constant.ActionType;
import com.aliyun.odps.mma.model.ActionLog;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.query.SourceFilter;

import java.util.List;
import java.util.Optional;

public interface DataSourceService {
    default Optional<DataSourceModel> getDataSource(String name) {
        return getDataSource(name, true);
    }
    default Optional<DataSourceModel> getDataSource(Integer id) {
        return getDataSource(id, true);
    }
    Optional<DataSourceModel> getDataSource(Integer id, boolean withConfig);
    Optional<DataSourceModel> getDataSource(String name, boolean withConfig);
    List<DataSourceModel> getDataSources(SourceFilter sourceFilter);
    void initSourceConfig(DataSourceModel ds);
    boolean isDataSourceExisted(String name);
    void insertDataSource(DataSourceModel ds);
    void updateLastUpdateTime(Integer id);
    void updateDSName(Integer id, String name);
    void updateDataSource(DataSourceModel ds);
    void setDataSourceInitOk(int sourceId);
    void setDataSourceInitRunning(int sourceId);
    void setDataSourceInitFailed(int sourceId);
    void addActionLog(ActionLog actionLog);
    List<ActionLog> getActionLogs(Integer sourceId, ActionType actionType);
}
