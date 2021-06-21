package com.aliyun.odps.mma.meta.transform;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.meta.transform.SchemaTransformerFactory;

public class SchemaTransformerFactoryTest {
  @Test
  public void testGetHiveSchemaTransformer() {
    Assert.assertTrue(
        SchemaTransformerFactory.get(DataSourceType.Hive) instanceof HiveSchemaTransformer);
  }

  @Test
  public void testGetMcSchemaTransformer() {
    Assert.assertTrue(
        SchemaTransformerFactory.get(DataSourceType.MaxCompute) instanceof McSchemaTransformer);
  }
}