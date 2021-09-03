/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.server.job;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.mma.config.DataDestType;
import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MetaDestType;
import com.aliyun.odps.mma.config.MetaSourceType;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.meta.MockMetaSource;
import com.aliyun.odps.mma.server.meta.MockMetaSourceFactory;

public class JobManagerTest {

  private static final String MYSQL_JDBC_CONN_URL = "jdbc:mysql://127.0.0.1:3306";
  private static final String MYSQL_JDBC_USERNAME = "root";
  private static final String MYSQL_JDBC_PASSWORD = "root";
  private static MetaManager META_MANAGER;

  @BeforeClass
  public static void beforeClass() {
    Map<String, String> builder = new HashMap<>();
    builder.put(MmaServerConfiguration.META_DB_JDBC_URL, MYSQL_JDBC_CONN_URL);
    builder.put(MmaServerConfiguration.META_DB_JDBC_USERNAME, MYSQL_JDBC_USERNAME);
    builder.put(MmaServerConfiguration.META_DB_JDBC_PASSWORD, MYSQL_JDBC_PASSWORD);
    MmaServerConfiguration.setInstance(builder);
    META_MANAGER = new MetaManager();

    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @AfterClass
  public static void afterClass() throws SQLException {
    try (Connection conn = DriverManager.getConnection(
        MYSQL_JDBC_CONN_URL, MYSQL_JDBC_USERNAME, MYSQL_JDBC_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DROP SCHEMA MMA");
      }
    }
  }

  @Test
  public void testAddNonPartitionedTableJob() throws Exception {
    JobManager jobManager = new JobManager(META_MANAGER, new MockMetaSourceFactory());

    Map<String, String> config = new HashMap<>();
    config.put(JobConfiguration.METADATA_SOURCE_TYPE, MetaSourceType.MaxCompute.name());
    config.put(JobConfiguration.DATA_SOURCE_TYPE, DataSourceType.MaxCompute.name());
    config.put(JobConfiguration.METADATA_DEST_TYPE, MetaDestType.OSS.name());
    config.put(JobConfiguration.DATA_DEST_TYPE, DataDestType.OSS.name());

    config.put(JobConfiguration.JOB_ID, "testAddNonPartitionedTableJob");
    config.put(JobConfiguration.OBJECT_TYPE, ObjectType.TABLE.name());
    config.put(JobConfiguration.SOURCE_CATALOG_NAME, MockMetaSource.DB_NAME);
    config.put(JobConfiguration.SOURCE_OBJECT_NAME, MockMetaSource.TBL_NON_PARTITIONED);
    config.put(JobConfiguration.DEST_CATALOG_NAME, MockMetaSource.DB_NAME);
    config.put(JobConfiguration.DEST_OBJECT_NAME, MockMetaSource.TBL_NON_PARTITIONED);

    JobConfiguration jobConfig = new JobConfiguration(config);
    String jobId = jobManager.addJob(jobConfig);
    Job job = jobManager.getJobById(jobId);
    JobConfiguration actual = job.getJobConfiguration();

    Assert.assertEquals("testAddNonPartitionedTableJob", job.getId());
    Assert.assertEquals(JobStatus.PENDING, job.getStatus());
    Assert.assertEquals(
        Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
        job.getPriority());
    Assert.assertFalse(job.hasSubJob());
    Assert.assertNull(job.getParentJob());
    Assert.assertEquals(config, actual);
  }

  @Test
  public void testAddPartitionedTableJob() throws Exception {
    JobManager jobManager = new JobManager(META_MANAGER, new MockMetaSourceFactory());

    Map<String, String> config = new HashMap<>();
    config.put(JobConfiguration.METADATA_SOURCE_TYPE, MetaSourceType.MaxCompute.name());
    config.put(JobConfiguration.DATA_SOURCE_TYPE, DataSourceType.MaxCompute.name());
    config.put(JobConfiguration.METADATA_DEST_TYPE, MetaDestType.OSS.name());
    config.put(JobConfiguration.DATA_DEST_TYPE, DataDestType.OSS.name());
    config.put(JobConfiguration.JOB_ID, "testAddPartitionedTableJob");
    config.put(JobConfiguration.OBJECT_TYPE, ObjectType.TABLE.name());
    config.put(JobConfiguration.SOURCE_CATALOG_NAME, MockMetaSource.DB_NAME);
    config.put(JobConfiguration.SOURCE_OBJECT_NAME, MockMetaSource.TBL_PARTITIONED);
    config.put(JobConfiguration.DEST_CATALOG_NAME, MockMetaSource.DB_NAME);
    config.put(JobConfiguration.DEST_OBJECT_NAME, MockMetaSource.TBL_PARTITIONED);

    JobConfiguration jobConfig = new JobConfiguration(config);
    String jobId = jobManager.addJob(jobConfig);
    Job job = jobManager.getJobById(jobId);
    JobConfiguration actual = job.getJobConfiguration();

    Assert.assertEquals("testAddPartitionedTableJob", job.getId());
    Assert.assertEquals(JobStatus.PENDING, job.getStatus());
    Assert.assertEquals(
        Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
        job.getPriority());
    Assert.assertTrue(job.hasSubJob());
    Assert.assertNull(job.getParentJob());
    Assert.assertEquals(config, actual);

    List<Job> subJobs = jobManager.listSubJobs(job);
    Assert.assertEquals(2, subJobs.size());

    Job subJob = subJobs.get(0);
    Assert.assertEquals("testAddPartitionedTableJob", subJob.getParentJob().getId());
    Assert.assertEquals(JobStatus.PENDING, subJob.getStatus());
    Assert.assertEquals(
        Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
        subJob.getPriority());
    Assert.assertFalse(subJob.hasSubJob());
    System.out.println(subJob.getJobConfiguration());
  }

  @Test
  public void testAddCatalogJob() throws Exception {
    JobManager jobManager = new JobManager(META_MANAGER, new MockMetaSourceFactory());

    Map<String, String> config = new HashMap<>();
    config.put(JobConfiguration.METADATA_SOURCE_TYPE, MetaSourceType.MaxCompute.name());
    config.put(JobConfiguration.DATA_SOURCE_TYPE, DataSourceType.MaxCompute.name());
    config.put(JobConfiguration.METADATA_DEST_TYPE, MetaDestType.OSS.name());
    config.put(JobConfiguration.DATA_DEST_TYPE, DataDestType.OSS.name());

    config.put(JobConfiguration.JOB_ID, "testAddCatalogJob");
    config.put(JobConfiguration.OBJECT_TYPE, ObjectType.CATALOG.name());
    config.put(JobConfiguration.SOURCE_CATALOG_NAME, MockMetaSource.DB_NAME);
    config.put(JobConfiguration.DEST_CATALOG_NAME, MockMetaSource.DB_NAME);
    config.put(JobConfiguration.SOURCE_OBJECT_TYPES, ObjectType.TABLE.name());

    JobConfiguration jobConfig = new JobConfiguration(config);
    String jobId = jobManager.addJob(jobConfig);
    Job job = jobManager.getJobById(jobId);

    Assert.assertEquals("testAddCatalogJob", job.getId());
    Assert.assertEquals(JobStatus.PENDING, job.getStatus());
    Assert.assertEquals(
        Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
        job.getPriority());
    Assert.assertTrue(job.hasSubJob());
    Assert.assertNull(job.getParentJob());

    List<Job> subJobs = jobManager.listSubJobs(job);
    Assert.assertEquals(2, subJobs.size());

    for (Job subJob : subJobs) {
      JobConfiguration subJobConfig = subJob.getJobConfiguration();
      System.out.println(subJob.getJobConfiguration());
      if (MockMetaSource.TBL_NON_PARTITIONED.equals(
          subJobConfig.get(JobConfiguration.SOURCE_OBJECT_NAME))) {
        Assert.assertEquals("testAddCatalogJob", subJob.getParentJob().getId());
        Assert.assertEquals(JobStatus.PENDING, subJob.getStatus());
        Assert.assertEquals(
            Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
            subJob.getPriority());
        Assert.assertFalse(subJob.hasSubJob());
      } else {
        TableMetaModel tableMetaModel =
            MockMetaSource.TBL_NAME_2_TBL_META.get(MockMetaSource.TBL_PARTITIONED);
        Assert.assertEquals("testAddCatalogJob", subJob.getParentJob().getId());
        Assert.assertEquals(JobStatus.PENDING, subJob.getStatus());
        Assert.assertEquals(
            Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
            subJob.getPriority());
        Assert.assertTrue(subJob.hasSubJob());
        List<Job> subSubJobs = jobManager.listSubJobs(subJob);
        Assert.assertEquals(tableMetaModel.getPartitions().size(), subSubJobs.size());
        Job subSubJob = subSubJobs.get(0);
        Assert.assertEquals(subJob.getId(), subSubJob.getParentJob().getId());
        Assert.assertEquals(JobStatus.PENDING, subSubJob.getStatus());
        Assert.assertEquals(
            Integer.valueOf(JobConfiguration.JOB_PRIORITY_DEFAULT_VALUE).intValue(),
            subSubJob.getPriority());
        Assert.assertFalse(subSubJob.hasSubJob());
        System.out.println(subSubJob.getJobConfiguration());
      }
    }
  }

  // TODO: remove
  // TODO: list
}
