package com.aliyun.odps.mma.meta.schema;

import com.aliyun.odps.*;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.mma.task.ClusterInfo;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.util.StringUtils;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
public class DstOdpsTableSchema extends TableSchema {

    public enum  DstOdpsTableType {
        /**
         * 普通表
         */
        COMMON("COMMON"),
        /**
         * ACID 1.0
         */
        ACID1("ACID1.0"),
        /**
         * ACID 2.0
         */
        DELTA("DELTA"),
        /**
         * APPEND2.0
         */
        APPEND2("APPEND2.0");

        final String value;

        DstOdpsTableType(String value) {
            this.value = value;
        }

        public static DstOdpsTableType fromValue(String value) {
            for (DstOdpsTableType dstOdpsTableType : DstOdpsTableType.values()) {
                if (dstOdpsTableType.value.equalsIgnoreCase(value)) {
                    return dstOdpsTableType;
                }
            }

            return DstOdpsTableType.valueOf(value);
        }
    }

    private String projectName;
    private String schemaName;
    private String tableName;

    private DstOdpsTableType tableType = DstOdpsTableType.COMMON;
    private List<String> primaryKeys;
    private Boolean enableTransaction;

    private String comment;
    private Integer lifeCycle;

    private ClusterInfo clusterInfo;

    private boolean autoPartition = false;
    private String autoPartitionBy;
    private String autoPartitionType;

    private String storageTierLifeCycleConfigJson;
    private StorageTierInfo.StorageTier storageTier;
    private boolean disableLifeCycle;
    private Map<String, String> tblProperties = new HashMap<>();

    public boolean isOriginal() {
        return tableType == DstOdpsTableType.COMMON;
    }

    public boolean isAcid1() {
        return tableType == DstOdpsTableType.ACID1;
    }

    public boolean isAcid2() {
        return tableType == DstOdpsTableType.DELTA;
    }

    public boolean compareWithDstOdpsTable(Table odpsTable, TaskProxy task) {
        TableSchema odpsTableSchema = odpsTable.getSchema();

        // 1. check column count & type
        if (getColumns().size() != odpsTableSchema.getColumns().size()) {
            Set<String> mmaNameMap = getColumns().stream().map(Column::getName).collect(Collectors.toSet());
            Set<String> odpsNameMap = odpsTableSchema.getColumns().stream().map(Column::getName).collect(Collectors.toSet());
            Set<String> notInOdps = new HashSet<>(mmaNameMap);
            notInOdps.removeAll(odpsNameMap);
            Set<String> notInMma = new HashSet<>(odpsNameMap);
            notInMma.removeAll(mmaNameMap);

            task.error("check schema", "column size not match, mma: "
                                       + getColumns().size()
                                       + ", odps: "
                                       + odpsTableSchema.getColumns().size()
                                       + " column not in odps");
            task.error("diff schema", "column in mma not in odps: "
                                      + String.join(", ", notInOdps)
                                      + ", column in odps not in mma: "
                                      + String.join(", ", notInMma));

            return false;
        }

        for (int i = 0; i < getColumns().size(); i++) {
            String mmaName = getColumn(i).getName();
            String mmaType = getColumn(i).getTypeInfo().getTypeName();
            String odpsName = odpsTableSchema.getColumn(i).getName();
            String odpsType = odpsTableSchema.getColumn(i).getTypeInfo().getTypeName();
            if (!mmaName.equalsIgnoreCase(odpsName) || !mmaType.equalsIgnoreCase(odpsType)) {
                task.error("check schema", "column type not match, mma column " +
                                           mmaName + " " + mmaType + ","
                                           + " odps column " + odpsName + " " + odpsType);
                return false;
            }
        }

        if (getPartitionColumns().size() != odpsTableSchema.getPartitionColumns().size()) {
            task.error("check schema",
                       "pt column size not match, mma: " + getPartitionColumns().size() + ", odps: " + odpsTableSchema.getPartitionColumns().size());
            return false;
        }

        for (int i = 0; i < getPartitionColumns().size(); i++) {
            String mmaColumnName = getPartitionColumn(i).getName();
            String mmaType = getPartitionColumn(i).getTypeInfo().getTypeName();
            String odpsColumnName = odpsTableSchema.getPartitionColumn(i).getName();
            String odpsType = odpsTableSchema.getPartitionColumn(i).getTypeInfo().getTypeName();
            if (!mmaColumnName.equalsIgnoreCase(odpsColumnName) || !mmaType.equalsIgnoreCase(odpsType)) {
                task.error("check schema", "partition column name not match, mma column " +
                                           mmaColumnName + ","
                                           + " odps column " + odpsColumnName);
                return false;
            }
        }

        // 2. check table type
        if (isAcid1()) {
            if (!odpsTable.isTransactional()) {
                task.error("check schema", "odps table exists, is not transactional table");
                return false;
            }

            if (odpsTable.isTransactional() && odpsTable.getPrimaryKey() != null && !odpsTable.getPrimaryKey().isEmpty()) {
                task.error("check schema", "odps table exists, is delta table, cannot be migrate as transactional table");
                return false;
            }
        }

        if (isAcid2()) {
            if (!odpsTable.isTransactional()) {
                task.error("check schema", "odps table exists, is not transactional table");
                return false;
            }

            if (odpsTable.isTransactional() && odpsTable.getPrimaryKey() != null && odpsTable.getPrimaryKey().isEmpty()) {
                task.error("check schema", "odps table exists, is transactional table, cannot be migrate as delta table");
                return false;
            }
        }

        if (isOriginal()) {
            if (odpsTable.isTransactional()) {
                task.error("check schema", "odps table exists, is not original table");
                return false;
            }
        }

        return true;
    }

    /**
     create [external] table [if not exists] <table_name>
     [primary key (<pk_col_name>, <pk_col_name2>),(<col_name> <data_type> [not null] [default <default_value>] [comment <col_comment>], ...)]
     [comment <table_comment>]
     [partitioned by (<col_name> <data_type> [comment <col_comment>], ...)]

     --用于创建聚簇表时设置表的Shuffle和Sort属性。
     [clustered by | range clustered by (<col_name> [, <col_name>, ...]) [sorted by (<col_name> [asc | desc] [, <col_name> [asc | desc] ...])] into <number_of_buckets> buckets]

     --指定表为Transactional表，后续可以对该表执行更新或删除表数据操作，但是Transactional表有部分使用限制，请根据需求创建。
     [tblproperties("transactional"="true")]

     --指定表为Delta Table表，结合primary key，后续可以做upsert，增量查询，time-travel等操作
     [tblproperties ("transactional"="true" [, "write.bucket.num" = "N", "acid.data.retain.hours"="hours"...])] [lifecycle <days>]
     ;
     * @return
     */
    public String getCreateTableSql() {
        StringBuilder sb = new StringBuilder();
        Map<String, String> tblProperties = new HashMap<>();

        // 1. create table [if not exists] <table_id>(
        sb.append("CREATE TABLE IF NOT EXISTS ");

        sb.append(quoteKwd(projectName)).append(".");
        if (StringUtils.isBlank(schemaName)) {
            sb.append(quoteKwd(tableName));
        } else {
            sb.append(quoteKwd(schemaName)).append(".")
                .append(quoteKwd(tableName));
        }
        sb.append(" (");


        // 2. column info
        List<Column> columns = getColumns();

        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            sb.append("\n")
                .append(quoteKwd(c.getName())).append(" ")
                .append(getTypeInfoNameWithBackQuotation(c.getTypeInfo()));

            // not null
            if (!c.isNullable() || isPkColumn (c.getName())) {
                sb.append(" ")
                    .append("NOT NULL");
            }

            // default value
            if (!StringUtils.isBlank(c.getDefaultValue())) {
                sb.append(" DEFAULT ");

                switch (c.getTypeInfo().getOdpsType()) {
                    case STRING:
                    case VARCHAR:
                    case CHAR:
                        sb.append("'")
                                .append(c.getDefaultValue())
                                .append("'");
                        break;
                    default:
                        sb.append(c.getDefaultValue());
                        break;
                }
            }

            if (!StringUtils.isBlank(c.getComment())) {
                sb.append(" COMMENT '").append(c.getComment()).append("'");
            }

            if (i + 1 < columns.size()) {
                sb.append(',');
            }
        }


        if (isAcid2()) {
            if (ListUtils.size(primaryKeys) > 0) {
                sb.append(", PRIMARY KEY(")
                    .append(String.join(", ", primaryKeys))
                    .append(") ");
            }
        }

        sb.append("\n)");

        if (!StringUtils.isBlank(comment)) {
            sb.append("\n COMMENT '").append(replaceComment(comment)).append("' ");
        }

        List<Column> pcolumns = getPartitionColumns();

        if (!pcolumns.isEmpty()) {
            if (autoPartition) {
                sb.append("\n AUTO PARTITIONED BY (TRUNC_TIME(")
                    .append(quoteKwd(autoPartitionBy))
                    .append(",'")
                    .append(autoPartitionType)
                    .append("'").append(")")
                    .append(" as ").append(pcolumns.get(0).getName()).append(")");
            } else {

                sb.append("\n PARTITIONED BY (");
                for (int i = 0; i < pcolumns.size(); i++) {
                    Column c = pcolumns.get(i);
                    sb.append(quoteKwd(c.getName())).append(" ")
                        .append(c.getTypeInfo().getTypeName());
                    if (!StringUtils.isBlank(c.getComment())) {
                        sb.append(" COMMENT '").append(c.getComment()).append("'");
                    }
                    if (i + 1 < pcolumns.size()) {
                        sb.append(',');
                    }
                }
                sb.append(")\n");
            }

            if (storageTierLifeCycleConfigJson != null) {
                tblProperties.put("lifecycle_config", storageTierLifeCycleConfigJson);
            }
        } else {
            if (storageTier != null) {
                // SQL 只支持小写
                tblProperties.put("storagetier", storageTier.getName().toLowerCase());
            }
        }

        if (isAcid1() || isAcid2()) {
            tblProperties.put("transactional", "true");
        }

        if (isOriginal()) {
            if (clusterInfo != null) {
                if (clusterInfo.isRangeCluster()) {
                    sb.append("\n").append("RANGE CLUSTERED BY ");
                } else if (clusterInfo.isHashCluster()) {
                    sb.append("\n").append("CLUSTERED BY ");
                }
                String names = "(" + clusterInfo.getColumnName().stream().map(
                    this::quoteKwd).collect(Collectors.joining(","))
                               + ")";
                sb.append(names)
                    .append(" SORTED BY ").append(names)
                    .append(" INTO ").append(clusterInfo.getBuckets()).append(" BUCKETS");
            }
        }

        tblProperties.putAll(this.tblProperties);
        if (!tblProperties.isEmpty()) {
            sb.append(" TBLPROPERTIES(");
            int idx = 0;
            for (Map.Entry<String, String> entry : tblProperties.entrySet()) {
                sb.append("'").append(entry.getKey()).append("'='").append(entry.getValue()).append("'");
                if (idx++ < tblProperties.size() - 1) {
                    sb.append(", ");
                }
            }
            sb.append(")");
        }

        if (Objects.nonNull(lifeCycle) && lifeCycle > 0) {
            sb.append(" LIFECYCLE ").append(lifeCycle);
        }


        sb.append(';');

        return sb.toString();
    }

    private boolean isPkColumn(String name) {
        return primaryKeys != null && primaryKeys.contains(name);
    }

    private String getTypeInfoNameWithBackQuotation(TypeInfo typeInfo) {
        switch (typeInfo.getOdpsType()) {
            case MAP:
                MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
                return OdpsType.MAP.name() + "<" + getTypeInfoNameWithBackQuotation(mapTypeInfo.getKeyTypeInfo()) + "," + getTypeInfoNameWithBackQuotation(mapTypeInfo.getValueTypeInfo()) + ">";
            case ARRAY:
                ArrayTypeInfo arrayTypeInfo = (ArrayTypeInfo) typeInfo;
                return OdpsType.ARRAY.name() + "<" + getTypeInfoNameWithBackQuotation(arrayTypeInfo.getElementTypeInfo()) + ">";
            case STRUCT:
                StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
                StringBuilder stringBuilder = new StringBuilder(OdpsType.STRUCT.name());
                stringBuilder.append("<");

                for (int i = 0; i < structTypeInfo.getFieldNames().size(); ++i) {
                    if (i > 0) {
                        stringBuilder.append(",");
                    }
                    stringBuilder.append("`").append(structTypeInfo.getFieldNames().get(i)).append("`");
                    stringBuilder.append(":");
                    stringBuilder.append(
                        getTypeInfoNameWithBackQuotation(structTypeInfo.getFieldTypeInfos().get(i)));
                }

                stringBuilder.append(">");

                return stringBuilder.toString();
            default:
                return typeInfo.getTypeName();
        }
    }

    static String replaceComment(String s) {
        return s.replace("\\", "\\\\")
            .replace("'", "\\'");
    }

    private String quoteKwd(String kwd) {
        return "`" + kwd + "`";
    }

    public static void main(String[] args) {
        System.out.println(DstOdpsTableType.valueOf ("ACID1.0"));
    }
}