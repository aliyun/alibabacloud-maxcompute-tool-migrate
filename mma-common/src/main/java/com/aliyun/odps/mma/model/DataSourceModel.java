package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.DataSourceInitStatus;
import com.aliyun.odps.mma.constant.SourceType;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.util.*;

@Data
public class DataSourceModel {
    private Integer id;
    private String name;
    private SourceType type;
    private Date lastUpdateTime;
    private Integer dbNum;
    private Integer tableNum;
    private Integer partitionNum;
    private DataSourceInitStatus initStatus;
    private Date createTime;
    private Date updateTime;
    @JsonIgnore
    private SourceConfig config;


    @JsonGetter
    public List<Map<String, Object>> config() {
        if (Objects.isNull(config)) {
            return null;
        }
        return config.toJsonObj();
    }

    @JsonIgnore
    public SourceConfig getConfig() {
        assert this.config != null;

        return this.config;
    }
}
