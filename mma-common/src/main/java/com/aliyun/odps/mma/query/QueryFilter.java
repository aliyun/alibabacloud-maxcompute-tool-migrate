package com.aliyun.odps.mma.query;

import lombok.Data;

@Data
public class QueryFilter {
    protected int current;
    protected int pageSize;

    public int getOffset() {
        return (current - 1) * pageSize;
    }
}
