package com.aliyun.odps.mma.orm;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.ActionStatus;
import com.aliyun.odps.mma.constant.ActionType;
import com.aliyun.odps.mma.datasource.DataSourceInitializer;
import com.aliyun.odps.mma.datasource.DataSourceUtils;
import com.aliyun.odps.mma.meta.DataSourceMetaLoader;
import com.aliyun.odps.mma.meta.MetaLoader;
import com.aliyun.odps.mma.meta.MetaLoaderUtils;
import com.aliyun.odps.mma.model.ActionLog;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;

@Component("MMADatasource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DataSource {
    DataSourceService ds;
    DataSourceMetaLoader dl;
    DataSourceInitializer di;
    DataSourceModel dm;
    MetaLoaderUtils metaLoaderUtils;
    DataSourceUtils dataSourceUtils;

    @Autowired
    public DataSource(
            DataSourceService ds,
            DataSourceMetaLoader dl,
            MetaLoaderUtils metaLoaderUtils,
            DataSourceUtils dataSourceUtils
    ) {
        this.ds = ds;
        this.dl = dl;
        this.metaLoaderUtils = metaLoaderUtils;
        this.dataSourceUtils = dataSourceUtils;
    }

    protected void init(String name) {
        Optional<DataSourceModel> dsModelOpt = ds.getDataSource(name);
        dsModelOpt.ifPresent(this::init);
    }

    protected void init(DataSourceModel dm) {
        this.dm = dm;

        if (Objects.isNull(dm.getConfig())) {
            ds.initSourceConfig(dm);
        }

        MetaLoader metaLoader = metaLoaderUtils.getMetaLoader(dm.getType());
        this.dl.setLoader(metaLoader);

        this.di = dataSourceUtils.getDataSource(dm.getType());
        if (Objects.nonNull(this.di)) {
            this.di.init(this);
        }
    }

    public boolean isExisted() {
        return Objects.nonNull(dm);
    }

    public void loadMeta() throws Exception {
        ds.updateLastUpdateTime(dm.getId());
        dl.open(dm);
        dl.updateData();
        dl.close();
    }

    public void runInitializer() throws Exception {
        if (Objects.isNull(this.di)) {
            return;
        }

        this.di.run();
    }

    public void setInitStatusOk() {
        ds.setDataSourceInitOk(dm.getId());
    }

    public void setInitStatusRunning() {
        ds.setDataSourceInitRunning(dm.getId());
    }

    public void setInitStatusFailed() {
        ds.setDataSourceInitFailed(dm.getId());
    }
    public SourceConfig getConfig() {
        return this.dm.getConfig();
    }

    public float getLoadingProgress() {
        return dl.getProgress();
    }

    public void log(ActionStatus status, String action, String msg, Object ...args) {
        ActionLog actionLog = ActionLog
                .builder()
                .sourceId(dm.getId())
                .actionType(ActionType.DATASOURCE_INIT)
                .status(status)
                .action(action)
                .msg(String.format(msg, args))
                .build();

        ds.addActionLog(actionLog);
    }

    public void start(String action, String msg, Object ...args) {
        log(ActionStatus.START, action, msg, args);
    }

    public void ok(String action, String msg, Object ...args) {
        log(ActionStatus.OK, action, msg, args);
    }

    public void failed(String action, String msg, Object ...args) {
        log(ActionStatus.FAILED, action, msg, args);
    }
}
