package com.aliyun.odps.mma.server;

import com.aliyun.odps.mma.config.DataSourceType;

public class SchemaTransformerFactory {

  private static final SchemaTransformer HIVE_SCHEMA_TRANSFORMER = new HiveSchemaTransformer();
  private static final SchemaTransformer MC_SCHEMA_TRANSFORMER = new McSchemaTransformer();

  public static SchemaTransformer get(DataSourceType dataSource) {
    switch (dataSource) {
      case Hive:
        return HIVE_SCHEMA_TRANSFORMER;
      case MaxCompute:
        return MC_SCHEMA_TRANSFORMER;
      case OSS:
        return MC_SCHEMA_TRANSFORMER;
      default:
        throw new IllegalArgumentException("Unsupported data source: " + dataSource);
    }
  }
}
