package com.aliyun.odps.mma.model;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.util.TableHasher;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

@Data
public class PartitionModel extends ModelBase {
    private Integer id;
    private Integer dbId;
    private Integer tableId;
    private String dbName;
    private String schemaName;
    private String tableName;
    private String value;

    public String getTableNameWithSchema() {
        if (Objects.nonNull(schemaName)) {
            return schemaName + "." + tableName;
        }

        return tableName;
    }

    public TableHasher tableTableHasher() {
        return new TableHasher(dbName, schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbName, tableName, schemaName, value);
    }

    @Override
    public boolean equals(Object other) {
        if (! (other instanceof PartitionModel)) {
            return false;
        }

        PartitionModel o = (PartitionModel) other;

        return Objects.equals(this.dbName, o.dbName)
                && Objects.equals(this.tableName, o.tableName)
                && Objects.equals(this.schemaName, o.schemaName)
                && Objects.equals(this.value, o.value);
    }

    @JsonIgnore
    public String key() {
        return String.format("%s.%s.%s", dbName, tableName, value);
    }

    // new value after replace
    public String getValue(SourceConfig sourceConfig) {
        Map<String, String> ptValueMapping = sourceConfig.getMap(SourceConfig.PT_VALUE_MAPPING);

        if (ptValueMapping.isEmpty()) {
            return this.value;
        }

        List<String> newValues = Arrays.stream(this.value.split("/"))
            .map(v -> {
                try {
                    return URLDecoder.decode(v, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.toList());
        for (Map.Entry<String, String> entry : ptValueMapping.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            newValues = newValues.stream().map(v -> v.replace(key, value))
                .collect(Collectors.toList());
        }

        return String.join("/", newValues);
    }

    // encoded value, safe to use / to split
    public String getValue() {
        return value;
    }

    // decoded original value
    public String getDecodedValue() {
        try {
            return URLDecoder.decode(value, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setValue(List<String> values) {
        this.value = values.stream().map(v -> {
                try {
                    return URLEncoder.encode(v, "UTF8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            })
            .collect(Collectors.joining("/"));
    }
}
