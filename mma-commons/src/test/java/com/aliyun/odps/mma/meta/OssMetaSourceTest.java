package com.aliyun.odps.mma.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.mma.TestUtils;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.OssMetaSource;
import com.aliyun.odps.mma.util.GsonUtils;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;

public class OssMetaSourceTest {
  private static OSS oss;
  private static OssMetaSource ossMetaSource;

  @BeforeClass
  public static void beforeClass() throws IOException {
    oss = new OSSClientBuilder().build(
        TestUtils.getProperty(TestUtils.OSS_ENDPOINT),
        TestUtils.getProperty(TestUtils.OSS_ACCESSKEY_ID),
        TestUtils.getProperty(TestUtils.OSS_ACCESSKEY_SECRET));

    // TODO: add objects
    // remove objects
    // put objects

    ossMetaSource = new OssMetaSource(
        TestUtils.getProperty(TestUtils.OSS_ACCESSKEY_ID),
        TestUtils.getProperty(TestUtils.OSS_ACCESSKEY_SECRET),
        TestUtils.getProperty(TestUtils.OSS_BUCKET),
        TestUtils.getProperty(TestUtils.OSS_PATH),
        TestUtils.getProperty(TestUtils.OSS_ENDPOINT));
  }

  @AfterClass
  public static void afterClass() throws IOException {
    // TODO: remove objects
    // list objects
    // remove objects
  }

  @Test
  public void testHasDatabase() {
    Assert.assertTrue(ossMetaSource.hasDatabase("mma_test_backup"));
  }

  @Test
  public void testHasTable() {
    boolean ret = ossMetaSource.hasTable("mma_test_backup", "test_text_1x1k");
    Assert.assertTrue(ret);
  }

  @Test
  public void testHasPartition() throws Exception {
    List<String> partitionVals = new ArrayList<>(2);
    partitionVals.add("yEybM");
    partitionVals.add("54");
    boolean ret = ossMetaSource.hasPartition(
        "mma_test_backup",
        "lzq_test_text_partitioned_10x1k", partitionVals);
    Assert.assertTrue(ret);
  }

  @Test
  public void testListDatabases() {
    List<String> databases = ossMetaSource.listDatabases();
    Assert.assertEquals(Collections.singletonList("mma_test_backup"), databases);
  }

  @Test
  public void testListTables() {
    List<String> tables = ossMetaSource.listTables("mma_test_backup");

    List<String> expected = new ArrayList<>(6);
    expected.add("lzq_test_non_partitioned_1x100k");
    expected.add("lzq_test_text_partitioned_10x1k");
    expected.add("test_non_partitioned_1x100k");
    expected.add("test_partitioned_100x10k");
    expected.add("test_text_1x1k");
    expected.add("test_text_partitioned_10x1k");

    Assert.assertEquals(expected, tables);
  }

  @Test
  public void testListPartitions() throws Exception {
    List<List<String>> partitionValsList = ossMetaSource.listPartitions(
        "mma_test_backup",
        "lzq_test_text_partitioned_10x1k");
    System.out.println(partitionValsList);
  }

  @Test
  public void testGetTableMeta() throws Exception {
    TableMetaModel tableMetaModel = ossMetaSource.getTableMeta(
        "mma_test_backup",
        "lzq_test_text_partitioned_10x1k");

    System.out.println(GsonUtils.GSON.toJson(tableMetaModel));
  }

  @Test
  public void testGetPartitionMeta() throws Exception {
    //[[yEybM, 54]]
    List<String> partitionVals = new ArrayList<>(2);
    partitionVals.add("yEybM");
    partitionVals.add("54");
    PartitionMetaModel partitionMetaModel = ossMetaSource.getPartitionMeta(
        "mma_test_backup",
        "lzq_test_text_partitioned_10x1k",
        partitionVals);
    System.out.println(GsonUtils.GSON.toJson(partitionMetaModel));
  }
}
