package com.aliyun.odps.mma.sql;

import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import lombok.Getter;

import java.util.List;

public class PartitionValue {
    @Getter
    private final List<MMAColumnSchema> columns;
    private final String[] values;

    /**
     * @param columns, odps等column需要先转换为MMAColumnSchema
     * @param partitionSpec 形如"p1=abc/p2=10"的形式
     */
    public PartitionValue(List<MMAColumnSchema> columns, String partitionSpec) {
        this.columns = columns;
        String[] keyValues = partitionSpec.split("/");

        assert this.columns.size() == keyValues.length;
        values = new String[keyValues.length];

        for (int i = 0; i < keyValues.length; i ++) {
            String[] kv = keyValues[i].split("=");
            MMAColumnSchema column = columns.get(i);
            assert column.getName().equals(kv[0]);
            values[i] = kv[1];
        }
    }

    public String transfer(TransferFunc transferFunc, String delimiter) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = columns.size(); i < n; i ++) {
            MMAColumnSchema column = columns.get(i);
            sb.append(transferFunc.call(column.getName(), column.getType(), values[i]));

            if (i < n - 1) {
                sb.append(delimiter);
            }
        }

        return sb.toString();
    }

    @FunctionalInterface
    public static interface TransferFunc {
        String call(String name, String type, String value);
    }
}
