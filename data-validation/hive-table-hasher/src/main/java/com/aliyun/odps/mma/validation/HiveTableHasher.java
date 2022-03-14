package com.aliyun.odps.mma.validation;

import com.aliyun.odps.Column;

import java.util.List;

public class HiveTableHasher extends TableHasher {
    public HiveTableHasher() {
        super();
    }

    @Override
    public void initFieldAdapter(List<Column> columns, List<Column> partitionColumn) {
        this.adapter = new HiveFiledAdapter(columns, partitionColumn);
    }
}
