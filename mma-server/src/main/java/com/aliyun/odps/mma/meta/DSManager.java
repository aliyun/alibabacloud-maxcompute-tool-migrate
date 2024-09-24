package com.aliyun.odps.mma.meta;

import com.aliyun.odps.mma.orm.DataSource;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.util.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class DSManager {
    private static final Logger logger = LoggerFactory.getLogger(DSManager.class);
    private ConcurrentMap<String, DataSource> dsLocks = new ConcurrentHashMap<>();
    private Map<String, String> dsErrors = new HashMap<>();
    private OrmFactory ormFactory;

    @Autowired
    public DSManager(OrmFactory ormFactory) {
         this.ormFactory = ormFactory;
    }

    public Result<Void, String> loadDataSourceMeta(String dsName)  {
        DsLock dsLock = tryLock(dsName);
        if (Objects.isNull(dsLock)) {
            return Result.err(String.format("%s is being loaded", dsName));
        }

        DataSource dataSource = dsLock.getDataSource();

        if (! dataSource.isExisted()) {
            return Result.err(String.format("%s has not been added", dsName));
        }

        Thread t = new Thread(() -> {
            try {
                dsErrors.remove(dsName);
                dataSource.loadMeta();
            } catch (Exception e) {
                logger.error("failed to load data source {}", dsName, e);
                dsErrors.put(dsName, String.format("failed: %s", e.getMessage()));
            } finally {
                try {
                    dsLock.close();
                } catch (Exception e) {
                    //!unreachable
                }
            }
        });

        t.start();

        return Result.ok();
    }

    public Result<Void, String> runInitializer(String dsName)  {
        DsLock dsLock = tryLock(dsName);
        if (Objects.isNull(dsLock)) {
            return Result.err(String.format("initializer of %s is running", dsName));
        }

        DataSource dataSource = dsLock.getDataSource();

        if (! dataSource.isExisted()) {
            return Result.err(String.format("%s has not been added", dsName));
        }

        try {
            dsErrors.remove(dsName);
            logger.info("start to run initializer of {}", dsName);
            dataSource.setInitStatusRunning();
            dataSource.runInitializer();
            dataSource.setInitStatusOk();
            logger.info("success to run initializer of {}", dsName);
        } catch (Exception e) {
            logger.error("failed to run {} initializer", dsName, e);
            dsErrors.put(dsName, String.format("failed: %s", e.getMessage()));
            dataSource.setInitStatusFailed();
            return Result.err(String.format("failed to run %s initializer: %s", dsName, e.getMessage()));
        } finally {
            try {
                dsLock.close();
            } catch (Exception e) {
                //!unreachable
            }
        }

        return Result.ok();
    }

    private DsLock tryLock(String name) {
        synchronized (this) {
            if (! dsLocks.containsKey(name)) {
                DataSource dataSource = ormFactory.newDataSource(name);
                dsLocks.put(name, dataSource);
                return new DsLock(this, name);
            }

            return null;
        }
    }

    private static class DsLock implements AutoCloseable {
        private final String name;
        private final DSManager dsManager;

        public DsLock(DSManager dsManager, String name) {
            this.name = name;
            this.dsManager = dsManager;
        }

        public DataSource getDataSource() {
            return this.dsManager.dsLocks.get(name);
        }

        @Override
        public void close() throws Exception {
            this.dsManager.dsLocks.remove(name);
        }
    }

    public float getProgress(String name) {
        DataSource ds = dsLocks.get(name);
        if (Objects.nonNull(ds)) {
            return ds.getLoadingProgress();
        }

        return -1;
    }

    public String getError(String name) {
        return dsErrors.get(name);
    }
}
