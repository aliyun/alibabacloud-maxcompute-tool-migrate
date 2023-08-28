package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.constant.MigrationStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

@Data
public class ModelBase {
    protected Integer sourceId;
    protected Long size;
    protected Long numRows;
    protected boolean updated;
    protected Date lastDdlTime;
    protected Date createTime;
    protected Date updateTime;
    protected String extra;
    protected MigrationStatus status = MigrationStatus.INIT;

    // 非数据库字段
    protected String sourceName;

    @JsonIgnore
    public Optional<Long> getSizeOpt() {
        if (Objects.nonNull(size)) {
            return Optional.of(size);
        }

        return Optional.empty();
    }

    @JsonIgnore
    public Optional<Long> getNumRowsOpt() {
        if (Objects.nonNull(numRows)) {
            return Optional.of(numRows);
        }

        return Optional.empty();
    }

    @JsonIgnore
    public Map<String, String> getExtraJson() {
        if (extra == null) {
            return new HashMap<>();
        }

        Gson gson = new Gson();
        return gson.fromJson(extra, new TypeToken<Map<String, String>>() {}.getType());
    }
}
