package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.TableSchema;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class MMAOdpsTableSchema extends TableSchema {
    private List<String> primaryKeys;
    private Boolean enableTransaction;
}