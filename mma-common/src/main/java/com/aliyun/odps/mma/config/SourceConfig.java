package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.constant.TaskType;
import com.aliyun.odps.mma.task.PartitionGrouping;

import java.util.List;

public abstract class SourceConfig extends Config  {
    @ConfigItem(desc = "定时更新", type="timer")
    public static String META_TIMER = "meta.timer";
    @ConfigItem(desc = "meta api访问并发量", type="int", defaultValue = "3")
    public static String META_API_BATCH = "meta.api.batch";
    private static final String DATABASE_WHITELIST = "source.database.whitelist";
    private static final String DATABASE_BLACKLIST = "source.database.blacklist";
    @ConfigItem(desc = "表黑名单, 格式为db.table", type="list", defaultValue = "[\"default\"]")
    protected static String TABLE_BLACKLIST = "source.table.blacklist";
    @ConfigItem(desc = "表白名单， 格式为db.table", type="list")
    public static String TABLE_WHITELIST = "source.table.whitelist";
    @ConfigItem(desc = "分区值转换配置（MaxCompute 分区值包含斜线）")
    public static String PT_VALUE_MAPPING = "partition.value.mapping";

    protected String sourceName;

    public abstract PartitionGrouping getPartitionGrouping();
    public abstract TaskType defaultTaskType();
    public abstract SourceType getSourceType();

    public SourceConfig() {
        super();
//        if (! this.getClass().equals(SourceConfig.class)) {
//            initConfigItemMap(SourceConfig.class);
//        }
    }

    public void setSourceName(String source) {
        this.sourceName = source;
    }

    public String getSourceName() {
        return this.sourceName;
    }

    public Integer getMetaApiBatch() {
        return this.getInteger(META_API_BATCH);
    }

    public List<String> getDbWhiteList() {
        return this.getList(DATABASE_WHITELIST);
    }

    public List<String> getDbBlackList() {
        return this.getList(DATABASE_BLACKLIST);
    }

    public List<String> getTableWhiteList() {
        return this.getList(TABLE_WHITELIST);
    }

    public List<String> getTableBlackList() {
        return this.getList(TABLE_BLACKLIST);
    }

    public String category() {
        return sourceName;
    }
}
