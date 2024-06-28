package com.aliyun.odps.mma.constant;

import java.util.HashMap;
import java.util.Map;

public class TaskTypeName {
    static Map<TaskType, String> map = new HashMap<TaskType, String>() {{
        put(TaskType.HIVE, "hive UDTF");
        put(TaskType.ODPS, "mc跨region");
        put(TaskType.ODPS_INSERT_OVERWRITE, "mc同region");
        put(TaskType.MC2MC_VERIFY, "mc校验");
        put(TaskType.HIVE_OSS, "OSS外表");
        put(TaskType.HIVE_MERGED_TRANS, "hive UDTF合并传输");
        put(TaskType.ODPS_MERGED_TRANS, "mc跨region合并传输");
        put(TaskType.DATABRICKS, "non-transaction table");
        put(TaskType.DATABRICKS_UDTF, "transaction table");
        put(TaskType.ODPS_OSS_TRANSFER, "OSS中转");
    }};

    public static String getName(TaskType type) {
        return map.get(type);
    }

    public static Map<TaskType, String> getNameMap() {
        return map;
    }
}
