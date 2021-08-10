package com.aliyun.odps.mma.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.server.SchemaTransformer.SchemaTransformResult;
import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel.TableMetaModelBuilder;
import com.google.gson.GsonBuilder;

public class HiveSchemaTransformerTest {
  @Test
  public void testTransformHiveSchema() {
    List<ColumnMetaModel> columnMetaModels = new ArrayList<>();
    columnMetaModels.add(new ColumnMetaModel("t_TINYINT", "TINYINT", null));
    columnMetaModels.add(new ColumnMetaModel("t_SMALLINT", "SMALLINT", null));
    columnMetaModels.add(new ColumnMetaModel("t_INT", "INT", null));
    columnMetaModels.add(new ColumnMetaModel("t_BIGINT", "BIGINT", null));
    columnMetaModels.add(new ColumnMetaModel("t_FLOAT", "FLOAT", null));
    columnMetaModels.add(new ColumnMetaModel("t_DOUBLE", "DOUBLE", null));
    columnMetaModels.add(new ColumnMetaModel("t_DOUBLE", "DOUBLE", null));
    columnMetaModels.add(new ColumnMetaModel("t_DECIMAL", "DECIMAL(10,2)", null));
    columnMetaModels.add(new ColumnMetaModel("t_STRING", "STRING", null));
    columnMetaModels.add(new ColumnMetaModel("t_VARCHAR", "VARCHAR(255)", null));
    columnMetaModels.add(new ColumnMetaModel("t_CHAR", "CHAR(255)", null));
    columnMetaModels.add(new ColumnMetaModel("t_TIMESTAMP", "TIMESTAMP", null));
    columnMetaModels.add(new ColumnMetaModel("t_DATE", "DATE", null));
    columnMetaModels.add(new ColumnMetaModel("t_BOOLEAN", "BOOLEAN", null));
    columnMetaModels.add(new ColumnMetaModel("t_BINARY", "BINARY", null));
    columnMetaModels.add(new ColumnMetaModel("t_ARRAY", "ARRAY<INT>", null));
    columnMetaModels.add(new ColumnMetaModel("t_MAP", "MAP<INT,STRING>", null));
    columnMetaModels.add(new ColumnMetaModel("t_STRUCT", "STRUCT<x:INT>", null));
    TableMetaModel tableMetaModel= new TableMetaModelBuilder(
        "test_db",
        "test_table",
        columnMetaModels).build();

    HiveSchemaTransformer hiveSchemaTransformer = new HiveSchemaTransformer();

    Map<String, String> configBuilder = new HashMap<>();
    configBuilder.put(JobConfiguration.OBJECT_TYPE, ObjectType.TABLE.name());
    configBuilder.put(JobConfiguration.DEST_CATALOG_NAME, "mc_test_db");
    configBuilder.put(JobConfiguration.DEST_OBJECT_NAME, "mc_test_table");
    JobConfiguration config = new JobConfiguration(configBuilder);
    SchemaTransformResult result =
        hiveSchemaTransformer.transform(tableMetaModel, config);
    System.out.println(new GsonBuilder().create().toJson(result.getTableMetaModel()));
  }
}