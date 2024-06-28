package com.aliyun.odps.mma.datasource;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.orm.DataSource;

public interface DataSourceInitializer {
    void init(DataSource dataSource);
    void verifyConfig() throws Exception;
    void run() throws Exception;
    SourceType sourceType();
}
