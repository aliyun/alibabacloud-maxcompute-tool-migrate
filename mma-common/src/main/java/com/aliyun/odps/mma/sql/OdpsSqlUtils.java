package com.aliyun.odps.mma.sql;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.meta.schema.MMAColumnSchema;
import com.aliyun.odps.mma.meta.schema.MMAOdpsTableSchema;
import com.aliyun.odps.mma.meta.schema.SchemaAdapterError;
import com.aliyun.odps.mma.util.ListUtils;
import com.aliyun.odps.mma.task.RangeClusterInfo;
import com.aliyun.odps.mma.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OdpsSqlUtils {

    public static String createTableSql(
            String projectName,
            String schemaName,
            String tableName,
            TableSchema tableSchema,
            String tableComment,
            Integer lifecycle,
            RangeClusterInfo rangeClusterInfo,
            List<String> blackList
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ");
        sb.append(projectName).append(".");

        if (StringUtils.isBlank(schemaName)) {
            sb.append(tableName);
        } else {
            sb.append(schemaName)
                    .append(".")
                    .append(tableName);
        }

        sb.append(" (");

        List<Column> columns = tableSchema.getColumns();
        if (blackList != null) {
            columns = columns.stream().filter(c -> !blackList.contains(c.getName())).collect(Collectors.toList());
        }
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            sb.append("\n`")
                    .append(c.getName()).append("` ")        // 列名
                    .append(c.getTypeInfo().getTypeName());  // 列类型

            // not null
            if (! c.isNullable()) {
                sb.append(" ")
                        .append("NOT NULL");
            }

            // default value, 默认值无论什么类型都用单引号(')包裹
            if (! StringUtils.isBlank(c.getDefaultValue())) {
                sb.append(" DEFAULT ")
                        .append("'")
                        .append(c.getDefaultValue())
                        .append("'");
            }

            if (! StringUtils.isBlank(c.getComment())) {
                sb.append(" COMMENT '").append(c.getComment()).append("'");
            }
            if (i + 1 < columns.size()) {
                sb.append(',');
            }
        }

        MMAOdpsTableSchema odpsTableSchema = (MMAOdpsTableSchema) tableSchema;
        boolean ts2 = Objects.nonNull(odpsTableSchema.getEnableTransaction()) && odpsTableSchema.getEnableTransaction();

        if (ts2) {
            List<String> primaryKeys = odpsTableSchema.getPrimaryKeys();

            if (! primaryKeys.isEmpty()) {
                sb.append(", PRIMARY KEY(")
                        .append(String.join(", ", primaryKeys))
                        .append(") ");
            }
        }

        sb.append("\n)");

        if (! StringUtils.isBlank(tableComment)) {
            sb.append("\n COMMENT '").append(tableComment).append("' ");
        }

        List<Column> pcolumns = tableSchema.getPartitionColumns();
        if (!pcolumns.isEmpty()) {
            sb.append("\n PARTITIONED BY (");
            for (int i = 0; i < pcolumns.size(); i++) {
                Column c = pcolumns.get(i);
                sb.append("`").append(c.getName()).append("` ")
                        .append(c.getTypeInfo().getTypeName());
                if (! StringUtils.isBlank(c.getComment())) {
                    sb.append(" COMMENT '").append(c.getComment()).append("'");
                }
                if (i + 1 < pcolumns.size()) {
                    sb.append(',');
                }
            }
            sb.append(")\n");
        }

        if (ts2) {
            sb.append(" TBLPROPERTIES(\"transactional\"=\"true\")\n");
        }

        if (Objects.nonNull(lifecycle) && lifecycle > 0) {
            sb.append(" LIFECYCLE ").append(lifecycle);
        }

        if (Objects.nonNull(rangeClusterInfo)) {
            sb.append("\n").append("RANGE CLUSTERED BY ").append("(").append(rangeClusterInfo.getColumnName()).append(")")
                    .append(" SORTED by ").append("(").append(rangeClusterInfo.getColumnName()).append(")")
                    .append(" INTO ").append(rangeClusterInfo.getBuckets()).append(" BUCKETS");
        }

        sb.append(';');

        return sb.toString();
    }

    public static String createExternalTableSql(
            String tableFullName,
            TableSchema tableSchema,
            String serde,
            Map<String, String> serdeProperties,
            String inputFormat,
            String outputFormat,
            String location
    ) {
        return createExternalTableSql(
                tableFullName,
                tableSchema,
                serde,
                serdeProperties,
                inputFormat,
                outputFormat,
                location,
                null
        );
    }

    public static String createExternalTableSql(
            String tableFullName,
            TableSchema tableSchema,
            String serde,
            Map<String, String> serdeProperties,
            String inputFormat,
            String outputFormat,
            String location,
            Integer lifecycle
    ) {
         /*
         CREATE EXTERNAL TABLE IF NOT EXISTS mma_test.`mma_temp_table_test_rcfile_partitioned_10x1k`(
             `t_tinyint` TINYINT,
             `t_decimal` DECIMAL(36,18),
             `t_struct` STRUCT<C1:STRING,C2:BIGINT>
         )
         PARTITIONED BY (
             `p1` STRING,
             `p2` BIGINT
         )
         ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
         WITH SERDEPROPERTIES (
             'field.delim'=',',
             'serialization.format'=',')
         STORED AS RCFILE
         LOCATION 'oss://ak:sk@endpoint/bucket/mma_test/test_rcfile_partitioned_10x1k'
         TBLPROPERTIES ('lifecycle.deletemeta'='true')
         LIFECYCLE 1;
        */
        StringBuilder sb = new StringBuilder(
                String.format("CREATE EXTERNAL TABLE IF NOT EXISTS %s(", tableFullName));

        Consumer<List<Column>> appendColumns = (columns) -> {
            for (Column c : columns) {
                sb.append(String.format("\n`%s` %s,", c.getName(), c.getTypeInfo().getTypeName()));
            }
            sb.deleteCharAt(sb.length() - 1);
        };

        appendColumns.accept(tableSchema.getColumns());
        sb.append("\n)");
        if (!tableSchema.getPartitionColumns().isEmpty()) {
            sb.append("\nPARTITIONED BY (");
            appendColumns.accept(tableSchema.getPartitionColumns());
            sb.append("\n)");
        }

        if (Objects.nonNull(serde)) {
            sb.append("\nROW FORMAT SERDE '").append(serde).append("'");
        }

        int propertiesNum = 0;
        if (Objects.nonNull(serdeProperties)) {
            propertiesNum = serdeProperties.size();
        }

        if (propertiesNum > 0) {
            int i = 0;
            sb.append("\nWITH SERDEPROPERTIES (");
            for (String key: serdeProperties.keySet()) {
                i += 1;
                String value = serdeProperties.get(key);

                // 目前只处理单个控制字符
                if (value.length() == 1) {
                    char v = value.charAt(0);

                    // 目前仅支持SOH控制字符
                    if ((int)v == 1) {
                        value = String.format("\\0%02o", (int)v);
                    }
                }

                sb.append(String.format("\n'%s'='%s'", key, value));

                if (i < propertiesNum) {
                    sb.append(",");
                }
            }
            sb.append(')');
        }
        sb.append("\nSTORED AS INPUTFORMAT '").append(inputFormat).append("'");
        sb.append("\nOUTPUTFORMAT '").append(outputFormat).append("'");
        sb.append("\nLOCATION '").append(location).append("'");

        if (Objects.nonNull(lifecycle) && lifecycle > 0) {
            sb.append("\nTBLPROPERTIES ('lifecycle.deletemeta'='true')");
            sb.append("\nLIFECYCLE ").append(lifecycle);
        }

        return sb.append(";").toString();
    }

    public static String addPartitionsSql(String tableFullName, List<PartitionValue> partitionValues) {
      /*
        ALTER TABLE mma_test.`table_name` ADD IF NOT EXISTS
        PARTITION (p1='KnPKw',p2=4204)
        PARTITION (p1='FfpLn',p2=1002);
       */

        StringBuilder sb = new StringBuilder(String.format("ALTER TABLE %s ADD IF NOT EXISTS", tableFullName));
        appendPartitionSpecs(sb, partitionValues, "\n");
        return sb.append(";").toString();
    }

    public static String truncateTableOrPartitionsSql(
            String tableFullName,
            List<PartitionValue> partitionValues
    ) {
        /*
         TRUNCATE TABLE mma_test.`table_name`
         PARTITION (p1='KnPKw',p2=4204),
         PARTITION (p1='FfpLn',p2=1002);
        */

        StringBuilder sb = new StringBuilder(String.format("TRUNCATE TABLE %s", tableFullName));
        if (Objects.nonNull(partitionValues) && !partitionValues.isEmpty()) {
            appendPartitionSpecs(sb, partitionValues, ",\n");
        }

        return sb.append(";").toString();
    }

    public static String selectCountSql(String tableFullName, List<PartitionValue> partitionValues) {
        /*
         SELECT COUNT(*) FROM mma_test.`test_rcfile_partitioned_10x1k`
         WHERE
         p1='dhcLk' AND p2=9505 OR
         p1='FfpLn' AND p2=1002;
         */

        StringBuilder sb = new StringBuilder(String.format("SELECT COUNT(*) from %s", tableFullName));

        if (Objects.nonNull(partitionValues) && !partitionValues.isEmpty()) {
            addSelectPartitionCondition(sb, partitionValues);
        }

        return sb.append(";").toString();
    }

    public static String selectCountByPtSql(String tableFullName, List<PartitionValue> partitionValues) {
        /*
         SELECT p1, p2, COUNT(*) FROM mma_test.`test_rcfile_partitioned_10x1k`
         WHERE
         p1='dhcLk' AND p2=9505 OR
         p1='FfpLn' AND p2=1002;
         */


        if (Objects.nonNull(partitionValues) && !partitionValues.isEmpty()) {
            String partitions = partitionValues
                    .get(0)
                    .getColumns()
                    .stream()
                    .map(MMAColumnSchema::getName)
                    .collect(Collectors.joining(" ,"));

            StringBuilder sb = new StringBuilder(String.format("SELECT %s, COUNT(*) from %s", partitions, tableFullName));
            addSelectPartitionCondition(sb, partitionValues);

            sb.append(" GROUP by ").append(partitions);
            return sb.append(";").toString();
        }

        return String.format("SELECT COUNT(*) from %s;", tableFullName);
    }

    public static String insertOverwriteSql(
            String sourceTable, String dstTable,
            TableSchema tableSchema,
            List<PartitionValue> partitionValues
    ) {
        /*
        INSERT OVERWRITE TABLE mma_test.`test_rcfile_partitioned_10x1k`
        PARTITION (`p1`, `p2`)
        SELECT * FROM mma_test.`mma_temp_table_test_rcfile_partitioned_10x1k`
        WHERE
        p1='yghpb' AND p2=6923 OR
        p1='FfpLn' AND p2=1002;
        */

        StringBuilder sb = new StringBuilder();

        List<String> partitionNames = tableSchema.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toList());

        sb.append(String.format("INSERT OVERWRITE TABLE %s", dstTable));
        if (!partitionNames.isEmpty()) {
            String partitionNamesStr = String.join(",", partitionNames);
            sb.append(" PARTITION (").append(partitionNamesStr).append(")");
        }

        List<String> columns = tableSchema.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        columns.addAll(partitionNames);

        sb.append("\nSELECT ")
                //.append(String.join(",", columns))
                .append("*")
                .append(" FROM ").append(sourceTable);

        if (!partitionValues.isEmpty()) {
            addSelectPartitionCondition(sb, partitionValues);
        }

        return sb.append(";").toString();
    }

    public static String adaptOdpsPartitionValue(String type, String value) {
        type = type.toUpperCase();

        if ("BIGINT".equals(type) || "INT".equals(type) || "SMALLINT".equals(type) || "TINYINT".equals(type)) {
            value = value.replaceFirst("^0+(?!$)", "");
            // Although the partition column type is integer, the partition values returned by HMS
            // client may have leading zeros. Let's say the partition in hive is hour=09. When the
            // partition is added in MC, the partition value will still be 09. In this example, however,
            // the UDTF will receive an integer 9 and creating an upload session with  9 will end up
            // with an error "No such partition". So, the leading zeros must be removed here.
        } else if ("STRING".equals(type) || type.startsWith("CHAR") || type.startsWith("VARCHAR")) {
            value = "'" + value + "'";
        }
        return value;
    }

    public static String dropPartitions(String tableFullName, List<PartitionValue> partitionValues) {
        StringBuilder sb = new StringBuilder();

        sb.append("ALTER TABLE ").append(tableFullName).append(" DROP IF EXISTS\n");
        appendPartitionSpecs(sb, partitionValues, ",");

        sb.append(";");

        return sb.toString();
    }

    private static void appendPartitionSpecs(StringBuilder sb, List<PartitionValue> partitionValues, String delimiter) {
        sb.append("\n");
        Stream<String> partitionSpecs = partitionValues.stream().map(pv -> pv.transfer(
                (name, type, value) -> String.format("%s=%s", name, adaptOdpsPartitionValue(type, value)),
                ","
        ));
        sb.append(partitionSpecs.map(spec -> String.format("PARTITION(%s)", spec))
                .collect(Collectors.joining(delimiter)));
    }

    private static void addSelectPartitionCondition(StringBuilder sb, List<PartitionValue> partitionValues) {
        /*
         WHERE
           p1='yghpb' AND p2=6923 OR
           p1='FfpLn' AND p2=1002;
         */
        sb.append("\nWHERE\n");
        List<String> pvs = partitionValues
                .stream()
                .map(pv -> pv.transfer(
                        (name, type, value) -> String.format("%s=%s", name, adaptOdpsPartitionValue(type, value)),
                        " AND "
                )).collect(Collectors.toList());

        sb.append(String.join(" OR\n", pvs));
    }
}

