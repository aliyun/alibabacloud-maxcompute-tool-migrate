package com.aliyun.odps.mma.datasource;

import com.aliyun.odps.mma.constant.SourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class DataSourceUtils {
    ApplicationContext appCtx;
    Map<SourceType, Class<? extends DataSourceInitializer>> typeMap = new HashMap<>();

    @Autowired
    public DataSourceUtils(ApplicationContext appCtx, List<DataSourceInitializer> dataSources) {
        this.appCtx = appCtx;

        for (DataSourceInitializer dataSource: dataSources) {
            typeMap.put(dataSource.sourceType(), dataSource.getClass());
        }
    }

    public DataSourceInitializer getDataSource(SourceType sourceType) {
        Class<? extends DataSourceInitializer> dsClass = typeMap.get(sourceType);

        if (Objects.isNull(dsClass)) {
            return null;
        }

        return appCtx.getBean(dsClass);
    }
}
