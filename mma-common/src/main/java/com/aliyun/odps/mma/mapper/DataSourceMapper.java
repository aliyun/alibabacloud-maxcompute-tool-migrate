package com.aliyun.odps.mma.mapper;

import com.aliyun.odps.mma.constant.ActionType;
import com.aliyun.odps.mma.constant.DataSourceInitStatus;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.model.ActionLog;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.query.SourceFilter;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public interface DataSourceMapper {
    void insertDataSource(DataSourceModel ds);
    DataSourceModel getDataSourceByName(@Param("name") String name);
    DataSourceModel getDataSourceById(@Param("id") Integer id);
    List<DataSourceModel> getDataSources(SourceFilter sourceFilter);
    SourceType getSourceType(@Param("name") String name);
    void updateLastUpdateTime(@Param("id") Integer id);
    void updateDSName(@Param("id") Integer id, @Param("name") String name);
    void updateDataSource(DataSourceModel ds);
    void setDataSourceInitStatus(@Param("sourceId") int sourceId, @Param("initStatus")DataSourceInitStatus initStatus);

    default void setDataSourceInitOk(int sourceId) {
        setDataSourceInitStatus(sourceId, DataSourceInitStatus.OK);
    }

    default void setDataSourceInitRunning(int sourceId) {
        setDataSourceInitStatus(sourceId, DataSourceInitStatus.RUNNING);
    }

    default void setDataSourceInitFailed(int sourceId) {
        setDataSourceInitStatus(sourceId, DataSourceInitStatus.FAILED);
    }

    void addActionLog(ActionLog actionLog);
    List<ActionLog> getActionLogs(@Param("sourceId") Integer sourceId, @Param("actionType")ActionType actionType);
}
