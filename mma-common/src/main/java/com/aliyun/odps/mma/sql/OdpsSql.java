package com.aliyun.odps.mma.sql;

import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.mma.config.JobConfig;
import com.aliyun.odps.mma.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OdpsSql {
    private final List<PartitionValue> partitionValues;
    private final List<PartitionValue> mergedPartitionValues;
    private final boolean partitionedTable;
    private final boolean hasPartitions;

    public OdpsSql(
            List<PartitionValue> partitionValues,
            List<PartitionValue> mergedPartitionValues,
            boolean partitionedTable,
            boolean hasPartitions
    ) {
        this.partitionValues = partitionValues;
        this.mergedPartitionValues = mergedPartitionValues;
        this.partitionedTable = partitionedTable;
        this.hasPartitions = hasPartitions;
    }

    public String createTableSql(String tableFullName, TableSchema tableSchema) {
        return  createTableSql(tableFullName, tableSchema, 0);
    }

    public String createTableSql(String tableFullName, TableSchema tableSchema, Integer maxPartitionLevel) {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("CREATE TABLE IF NOT EXISTS %s", tableFullName));

        if (! partitionedTable) {
            appendColumns(sb, tableSchema.getColumns());
            return sb.append(";").toString();
        }

        List<Column> columns = new ArrayList<>(tableSchema.getColumns().size());
        columns.addAll(tableSchema.getColumns());

        List<Column> ptColumns = tableSchema.getPartitionColumns();

        if (maxPartitionLevel > 0 && ptColumns.size() > maxPartitionLevel) {
            columns.addAll(ptColumns.subList(maxPartitionLevel, ptColumns.size()));
            ptColumns = ptColumns.subList(0, maxPartitionLevel);
        }

        appendColumns(sb, columns);
        sb.append("\nPARTITIONED BY ");
        appendColumns(sb,  ptColumns);

        return sb.append(";").toString();
    }


    public String createExternalTableSql(String tempTableName,
                                         TableSchema tableSchema,
                                         String serde,
                                         Map<String, String> serdeProperties,
                                         String inputFormat,
                                         String outputFormat,
                                         String location) {
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
         LOCATION 'oss://ak:sk@endpoint/bucket/mma_test/test_rcfile_partitioned_10x1k';
        */
        StringBuilder sb = new StringBuilder();

        createTableCommon(sb, tempTableName, tableSchema, true);

        if (null != serde) {
            sb.append("\nROW FORMAT SERDE '").append(serde).append("'");
        }

        int propertiesNum = serdeProperties.size();
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

        return sb.append(";").toString();
    }

    private void createTableCommon(StringBuilder sb, String tableFullName, TableSchema tableSchema, boolean external) {
        String externalStr = external ? "EXTERNAL" : "";
        sb.append(String.format("CREATE %s TABLE IF NOT EXISTS %s", externalStr, tableFullName));
        appendColumns(sb, tableSchema.getColumns());

        if (partitionedTable) {
            sb.append("\nPARTITIONED BY ");
            appendColumns(sb, tableSchema.getPartitionColumns());
        }
    }

    private void appendColumns(StringBuilder sb, List<Column> columns) {
        /*
        (
          `c1` TYPE1,
          `c2` TYPE2,
          ...
          `cn` TYPEN
          )
         */
        sb.append('(');
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            sb.append(String.format("%n`%s` %s", c.getName(), c.getTypeInfo().getTypeName()));
            if (!StringUtils.isBlank(c.getComment())) {
                sb.append(" COMMENT '").append(c.getComment()).append("'");
            }
            if (i + 1 < columns.size()) {
                sb.append(',');
            }
        }
        sb.append("\n)");
    }

    public String addPartitionsSql(String tableName) {
        /*
        ALTER TABLE mma_test.`table_name` ADD IF NOT EXISTS
        PARTITION (p1='KnPKw',p2=4204)
        PARTITION (p1='FfpLn',p2=1002);
         */
        StringBuilder sb = new StringBuilder(String.format("ALTER TABLE %s ADD IF NOT EXISTS", tableName));
        appendDDLPartitionCondition(sb, "\n");
        return sb.append(";").toString();
    }

    public String truncateSql(String tableFullName) {
        /*
        TRUNCATE TABLE mma_test.`table_name`
        PARTITION (p1='KnPKw',p2=4204),
        PARTITION (p1='FfpLn',p2=1002);
         */
        if (partitionedTable && !hasPartitions) {
            // 如果是分区表，但又没有分区，直接执行truncate table的话会把整个table truncate掉，所以这里禁止这种操作。
            // 返回空字符串后会是执行sql的地方报错。为了防止这个方法被误用，强制报错。
            return "";
        }

        StringBuilder sb = new StringBuilder(String.format("truncate table %s", tableFullName));
        appendDDLPartitionCondition(sb, ",\n");
        return sb.toString().trim() + ";";
    }

    public String selectCountSql(String tableFullName) {
        /*
        SELECT COUNT(*) FROM mma_test.`test_rcfile_partitioned_10x1k`
        WHERE
        p1='dhcLk' AND p2=9505 OR
        p1='FfpLn' AND p2=1002;
         */
        StringBuilder sb = new StringBuilder(String.format("SELECT COUNT(*) from %s", tableFullName));
        addSelectPartitionCondition(sb);
        return sb.append(";").toString();
    }

    public String insertOverwriteSql(String source, String dest) {
        /*
        INSERT OVERWRITE TABLE mma_test.`test_rcfile_partitioned_10x1k`
        PARTITION (`p1`, `p2`)
        SELECT * FROM mma_test.`mma_temp_table_test_rcfile_partitioned_10x1k`
        WHERE
        p1='yghpb' AND p2=6923 OR
        p1='FfpLn' AND p2=1002;
         */
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("INSERT OVERWRITE TABLE %s", dest));
        if (hasPartitions) {
            String ptNames = partitionValues
                    .stream()
                    .map(pv -> pv.transfer((name, type, value) -> String.format("`%s`", name), ", "))
                    .collect(Collectors.toList()).get(0);
            sb.append(" PARTITION (").append(ptNames).append(")");
        }
        sb.append("\nSELECT * FROM ").append(source);
        addSelectPartitionCondition(sb);

        return sb.append(";").toString();
    }

    private void addSelectPartitionCondition(StringBuilder sb) {
        /*
         WHERE
           p1='yghpb' AND p2=6923 OR
           p1='FfpLn' AND p2=1002;
         */
        if (!hasPartitions) {
            return;
        }

        sb.append("\nWHERE\n");
        List<String> pvs = partitionValues
          .stream()
          .map(pv -> pv.transfer(
            (name, type, value) -> String.format("%s=%s", name, adaptOdpsPartitionValue(type, value)),
            " AND "
          )).collect(Collectors.toList());

        sb.append(String.join(" OR\n", pvs));
    }

    private void appendDDLPartitionCondition(StringBuilder sb, String delimiter) {
        /*
        \nPARTITION (p1='KnPKw',p2=4204)<delimiter>
        PARTITION (p1='FfpLn',p2=1002)
         */
        if (!hasPartitions) {
            return;
        }

        sb.append("\n");

        List<PartitionValue> ptValues = mergedPartitionValues;
        if (Objects.isNull(ptValues)) {
            ptValues = partitionValues;
        }

        Stream<String> partitionSpecs = ptValues.stream().map(pv -> pv.transfer(
          (name, type, value) -> String.format("%s=%s", name, adaptOdpsPartitionValue(type, value)),
          ","
        ));
        sb.append(partitionSpecs.map(spec -> String.format("PARTITION(%s)", spec))
                    .collect(Collectors.joining(delimiter)));
    }

    public static String adaptOdpsPartitionValue(String type, String value) {
        if ("BIGINT".equalsIgnoreCase(type) || "INT".equalsIgnoreCase(type)
            || "SMALLINT".equalsIgnoreCase(type) || "TINYINT".equalsIgnoreCase(type)) {
            value = value.replaceFirst("^0+(?!$)", "");
        // Although the partition column type is integer, the partition values returned by HMS
        // client may have leading zeros. Let's say the partition in hive is hour=09. When the
        // partition is added in MC, the partition value will still be 09. In this example, however,
        // the UDTF will receive an integer 9 and creating an upload session with  9 will end up
        // with an error "No such partition". So, the leading zeros must be removed here.
        } else if ("STRING".equalsIgnoreCase(type)) {
            value = "'" + value + "'";
        }
        return value;
    }
}
