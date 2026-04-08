package com.aliyun.odps.mma;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.datasource.DataSourceInitializer;
import com.aliyun.odps.mma.meta.HiveUtils;
import com.aliyun.odps.mma.orm.DataSource;
import com.aliyun.odps.mma.util.ExceptionUtils;

@Primary
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveSourceInitializer implements DataSourceInitializer {
    private DataSource dataSource;
    private HiveUtils hiveUtils;
    private HiveConfig config;
    private final MMAConfig mmaConfig;

    public HiveSourceInitializer(@Autowired MMAConfig mmaConfig) {
        this.mmaConfig = mmaConfig;
    }

    public void init(DataSource dataSource) {
        this.dataSource = dataSource;
        this.hiveUtils = new HiveUtils((HiveConfig) dataSource.getConfig());
        this.config = (HiveConfig) dataSource.getConfig();
    }

    @Override
    public void verifyConfig() throws Exception {
    }

    @Override
    public void run() throws Exception {
        AtomicBoolean notExist = new AtomicBoolean();

        String ossUrl = config.getConfig(HiveConfig.HIVE_UDTF_JAR_OSS_URL);
        String functionName = dataSource.getConfig().getOrDefault(HiveConfig.HIVE_UDTF_NAME, "default.odps_data_dump_multi");
        String classPath = dataSource.getConfig().getOrDefault(HiveConfig.HIVE_UDTF_CLASS, "hive.com.aliyun.odps.mma.io.McDataTransmissionUDTF");

        // 给cluster添加依赖
        withLog(
                ()-> {
                    hiveUtils.executeQuery("desc function " + functionName, "desc function", new HashMap<>(), (rs) -> {
                        notExist.set(rs.getString(1).contains("does not exist"));
                    });
                },
                "check function exists",
                () -> String.valueOf(notExist.get())
        );

        if (notExist.get()) {
            String sql = "CREATE FUNCTION " + functionName +
                         " as '" + classPath +
                         "' USING " + "JAR '" + ossUrl + "'";
            withLog(
                () -> {
                    hiveUtils.execute(sql, new HashMap<>());
                },
                "create function",
                () -> sql
            );
        }

        // reload functions
        withLog(
                () -> {
                   hiveUtils.execute("reload functions", new HashMap<>());
                },
                "reload functions",
                () -> ""
        );
    }

    @Override
    public SourceType sourceType() {
        return SourceType.HIVE;
    }

    private void withLog(ActionFunc actionFunc, String action, Supplier<String> msg) throws Exception {
        try {
            dataSource.start(action, msg.get());
            actionFunc.call();
            dataSource.ok(action, msg.get());
        } catch (Exception e) {
            dataSource.failed(action , msg.get() + ", detail=%s", ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    @FunctionalInterface
    private static interface ActionFunc {
        void call() throws Exception;
    }
}
