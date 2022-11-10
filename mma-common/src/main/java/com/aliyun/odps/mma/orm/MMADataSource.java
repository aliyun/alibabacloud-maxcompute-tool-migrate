package com.aliyun.odps.mma.orm;

import com.aliyun.odps.mma.meta.DataSourceMetaLoader;
import com.aliyun.odps.mma.meta.MetaLoader;
import com.aliyun.odps.mma.meta.MetaLoaderUtils;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.DataSourceModel;
import com.aliyun.odps.mma.service.DataSourceService;
import com.aliyun.odps.mma.service.DbService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.Optional;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MMADataSource {
    DataSourceService ds;
    DbService dbService;
    DataSourceMetaLoader dl;
    DataSourceModel dm;
    MetaLoaderUtils metaLoaderUtils;

    @Autowired
    public MMADataSource(
            DataSourceService ds,
            DataSourceMetaLoader dl,
            MetaLoaderUtils metaLoaderUtils,
            DbService dbService
    ) {
        this.ds = ds;
        this.dl = dl;
        this.metaLoaderUtils = metaLoaderUtils;
        this.dbService = dbService;
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

    public float getLoadingProgress() {
        return dl.getProgress();
    }

    public boolean hasDb(String dbName) {
        return dbService.getDbByName(dm.getName(), dbName).isPresent();
    }

    public DataBaseModel getDb(String dbName) {
        Optional<DataBaseModel> dmOpt = dbService.getDbByName(dm.getName(), dbName);
        if (dmOpt.isPresent()) {
            return dmOpt.get();
        }

        return null;
    }
}
