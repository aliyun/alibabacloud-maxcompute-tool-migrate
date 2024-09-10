package com.aliyun.odps.mma.constant;

import com.aliyun.odps.mma.util.I18nUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class TaskTypeName {
    private final I18nUtils i18nUtils;

    @Autowired
    public TaskTypeName(I18nUtils i18nUtils) {
        this.i18nUtils = i18nUtils;
    }

    static Map<TaskType, String> map = new HashMap<TaskType, String>() {{
        put(TaskType.HIVE, "hive UDTF");
        put(TaskType.ODPS, "mc跨region");
        put(TaskType.ODPS_INSERT_OVERWRITE, "mc同region");
        put(TaskType.MC2MC_VERIFY, "mc校验");
        put(TaskType.HIVE_OSS, "OSS外表");
        put(TaskType.HIVE_MERGED_TRANS, "hive UDTF合并传输");
        put(TaskType.ODPS_MERGED_TRANS, "mc跨region合并传输");
        put(TaskType.DATABRICKS, "Databricks spark");
        put(TaskType.DATABRICKS_UDTF, "Databricks hive UDTF");
        put(TaskType.BIGQUERY, "使用 Storage Read API 进行数据迁移");
        put(TaskType.ODPS_OSS_TRANSFER, "OSS中转");
    }};

    public String getName(TaskType type, String lang) {
        return i18nUtils.get("task.name." + type.toString().toLowerCase(), lang, map.get(type));
    }

    public Map<TaskType, String> getNameMap(String lang) {
        Map<TaskType, String> nameMap = new HashMap<>(map.size());

        for (TaskType type: map.keySet()) {
            nameMap.put(type, getName(type, lang));
        }

        return nameMap;
    }
}
