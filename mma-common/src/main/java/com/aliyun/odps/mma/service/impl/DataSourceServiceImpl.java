package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.config.SourceConfigUtils;
import com.aliyun.odps.mma.constant.ActionType;
import com.aliyun.odps.mma.mapper.ConfigMapper;
import com.aliyun.odps.mma.mapper.DataSourceMapper;
import com.aliyun.odps.mma.model.ActionLog;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.query.SourceFilter;
import com.aliyun.odps.mma.service.DataSourceService;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
public class DataSourceServiceImpl implements DataSourceService {
    private final DataSourceMapper mapper;
    private final SourceConfigUtils sourceConfigUtils;
    SqlSessionFactory sqlSessionFactory;

    @Autowired
    public DataSourceServiceImpl(
            DataSourceMapper mapper,
            SourceConfigUtils sourceConfigUtils,
            SqlSessionFactory sqlSessionFactory
    ) {
        this.mapper = mapper;
        this.sourceConfigUtils = sourceConfigUtils;
        this.sqlSessionFactory = sqlSessionFactory;
    }

    @Override
    public Optional<DataSourceModel> getDataSource(String name, boolean withConfig) {
        DataSourceModel ds = mapper.getDataSourceByName(name);

        if (Objects.isNull(ds)) {
            return Optional.empty();
        }

        if (withConfig) {
            this.initSourceConfig(ds);
        }

        return Optional.of(ds);
    }

    @Override
    public Optional<DataSourceModel> getDataSource(Integer id, boolean withConfig) {
        DataSourceModel ds = mapper.getDataSourceById(id);

        if (Objects.isNull(ds)) {
            return Optional.empty();
        }

        if (withConfig) {
            this.initSourceConfig(ds);
        }

        return Optional.of(ds);
    }

    @Override
    public List<DataSourceModel> getDataSources(SourceFilter sourceFilter) {
        return mapper.getDataSources(sourceFilter);
    }

    public void initSourceConfig(DataSourceModel ds) {
        SourceConfig config = sourceConfigUtils.newSourceConfig(ds.getType(), ds.getName());
        ds.setConfig(config);
    }

    @Override
    public boolean isDataSourceExisted(String name) {
        DataSourceModel ds = mapper.getDataSourceByName(name);
        return Objects.nonNull(ds);
    }

    @Override
    public void insertDataSource(DataSourceModel ds) {
        this.mapper.insertDataSource(ds);
    }

    @Override
    public void updateLastUpdateTime(Integer id) {
        this.mapper.updateLastUpdateTime(id);
    }

    @Override
    public void updateDSName(Integer id, String name) {
        DataSourceModel ds =  this.mapper.getDataSourceById(id);

        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            ConfigMapper configMapper = session.getMapper(ConfigMapper.class);
            DataSourceMapper dsMapper = session.getMapper(DataSourceMapper.class);

            dsMapper.updateDSName(id, name);
            configMapper.updateCategory(ds.getName(), name);
        }
    }

    @Override
    public void updateDataSource(DataSourceModel ds) {
        this.mapper.updateDataSource(ds);
    }

    @Override
    public void setDataSourceInitOk(int sourceId) {
        this.mapper.setDataSourceInitOk(sourceId);
    }

    @Override
    public void setDataSourceInitRunning(int sourceId) {
        this.mapper.setDataSourceInitRunning(sourceId);
    }

    @Override
    public void setDataSourceInitFailed(int sourceId) {
        this.mapper.setDataSourceInitFailed(sourceId);
    }

    @Override
    public void addActionLog(ActionLog actionLog) {
        this.mapper.addActionLog(actionLog);
    }

    @Override
    public List<ActionLog> getActionLogs(Integer sourceId, ActionType actionType) {
        return this.mapper.getActionLogs(sourceId, actionType);
    }
}
