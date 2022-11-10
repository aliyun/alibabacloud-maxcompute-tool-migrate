package com.aliyun.odps.mma.query;

import lombok.Data;

@Data
public class SortPair {
    private String name;
    private String order;

    public SortPair(String name, String order) {
        this.name = name;
        this.order = order;
    }
}
