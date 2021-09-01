package com.aliyun.odps.mma.config;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.aliyun.odps.mma.TestUtils;
import com.aliyun.odps.mma.exception.MmaException;

/**
 * Requirements:
 *   1. SSH tunneling (for hive). E.g. ssh -NL 10000:${DESTINATION}:10000 ${USER}@${REMOTE}
 */
public class ConfigurationUtilsTest {

  private static class CmpCase {
    List<String> begin;
    List<String> end;
    boolean equal;
    boolean greater;

    public CmpCase(String caseLine) {
      String[] cases = caseLine.split(";");
      begin = Arrays.asList(cases[0].split("/"));
      end = Arrays.asList(cases[1].split("/"));
      equal = "=".equals(cases[2]);
      greater = ">".equals(cases[2]);
    }
  }

  @Test
  public void testPartitionComparator() {
    List<PartitionOrderType> orderTypes = new ArrayList<>();
    orderTypes.add(PartitionOrderType.lex);
    orderTypes.add(PartitionOrderType.num);
    orderTypes.add(PartitionOrderType.num);
    ConfigurationUtils.PartitionComparator cmp = new ConfigurationUtils.PartitionComparator(orderTypes);

    List<CmpCase> testCases = new ArrayList<>();
    testCases.add(new CmpCase("abc;abc;="));
    testCases.add(new CmpCase("abc/123/456;abc/123/456;="));
    testCases.add(new CmpCase("abc/123;abc/123/456;="));
    testCases.add(new CmpCase("abc/10;abc/7;>"));
    testCases.add(new CmpCase("10/123;7/123;<"));


    for (CmpCase cmpCase: testCases) {
      int ans = cmp.compare(cmpCase.begin, cmpCase.end);
      assertEquals(ans == 0, cmpCase.equal);
      assertEquals(ans > 0, cmpCase.greater);
    }
  }

  @Test
  public void testValidateMcCredentials() throws IOException {
    TestUtils.printProperties();
    try {
      ConfigurationUtils.validateMcCredentials(
          TestUtils.getProperty(TestUtils.MC_ENDPOINT),
          TestUtils.getProperty(TestUtils.MC_ACCESSKEY_ID),
          TestUtils.getProperty(TestUtils.MC_ACCESSKEY_SECRET));
    } catch (MmaException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testValidateHiveMetastoreCredentials() throws IOException {
    TestUtils.printProperties();
    try {
      Map<String, String> configBuilder = new HashMap<>();
      configBuilder.put(AbstractConfiguration.METADATA_SOURCE_TYPE, MetaSourceType.Hive.name());
      configBuilder.put(AbstractConfiguration.METADATA_SOURCE_HIVE_IMPL, "HMS");
      configBuilder.put(
          AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_URIS,
          TestUtils.getProperty(TestUtils.HIVE_METASTORE_URIS));
      ConfigurationUtils.validateHiveMetastoreCredentials(new JobConfiguration(configBuilder));
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testValidateHiveJdbcCredentials() throws IOException {
    TestUtils.printProperties();
    try {
      ConfigurationUtils.validateHiveJdbcCredentials(
          TestUtils.getProperty(TestUtils.HIVE_JDBC_URL),
          TestUtils.getProperty(TestUtils.HIVE_JDBC_USERNAME),
          TestUtils.getProperty(TestUtils.HIVE_JDBC_PASSWORD));
    } catch (MmaException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testValidateOssCredentials() throws IOException {
    TestUtils.printProperties();
    try {
      ConfigurationUtils.validateOssCredentials(
          TestUtils.getProperty(TestUtils.OSS_ENDPOINT),
          TestUtils.getProperty(TestUtils.OSS_BUCKET),
          TestUtils.getProperty(TestUtils.OSS_ACCESSKEY_ID),
          TestUtils.getProperty(TestUtils.OSS_ACCESSKEY_SECRET));
    } catch (MmaException e) {
      e.printStackTrace();
      fail();
    }
  }
}