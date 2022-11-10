package com.aliyun.odps.mma.model;

import lombok.Data;

import java.util.Objects;


@Data
public class DataBaseModel extends ModelBase  {
    private Integer id;
    private String name;
    private String description;
    private String owner;

    // 非数据库字段
    private int tables;
    private int tablesDoing;
    private int tablesDone;
    private int tablesPartDone;
    private int tablesFailed;
    private int partitions;
    private int partitionsDoing;
    private int partitionsDone;
    private int partitionsFailed;

    public DataBaseModel() {
        super();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof DataBaseModel)) {
            return false;
        }

        DataBaseModel o = (DataBaseModel) other;

        return Objects.equals(name, o.name);
    }
}
