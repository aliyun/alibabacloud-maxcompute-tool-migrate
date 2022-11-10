package com.aliyun.odps.mma.constant;

public enum TaskType {
    MOCK,
    /**
     * hive udtf task
     */
    HIVE,
    /**
     * hive datax task
     */
    HIVE_DATAX,
    /**
     * odps Copy Task
     */
    ODPS,
    /**
     * odps simple insert overwrite task
     */
    ODPS_INSERT_OVERWRITE,
    MC2MC_VERIFY,
    /**
     *
     */
    OSS,
    HIVE_OSS
}
