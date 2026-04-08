package com.aliyun.odps.mma.util;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
public class MysqlConfig {
    private final int maxBatchSize;

    public MysqlConfig(@Value("${MYSQL_MAX_BATCH_SIZE:10000}") int mysqlMaxBatchSize) {
        this.maxBatchSize = mysqlMaxBatchSize;
    }
}
