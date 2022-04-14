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

package com.aliyun.odps.mma.server.meta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;

public class MetaManagerTest {

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
  public static void AfterClass() throws SQLException {
    try (Connection conn = DriverManager.getConnection(
        MYSQL_JDBC_CONN_URL, MYSQL_JDBC_USERNAME, MYSQL_JDBC_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("DROP SCHEMA MMA");
      }
    }
  }

  @Test
  public void testAddAndRemoveJob() {
    addJob("testAddAndRemoveJob");
    removeJob("testAddAndRemoveJob");
  }

  @Test
  public void testAddAndRemoveSubJob() {
    addSubJob("testAddAndRemoveSubJob", "S-testAddAndRemoveSubJob");
    addJob("testAddAndRemoveSubJob");

    removeSubJob("testAddAndRemoveSubJob", "S-testAddAndRemoveSubJob");
    removeJob("testAddAndRemoveSubJob");
  }

  @Test
  public void testUpdateJob() {
    addJob("testUpdateJob");

    JobRecord jobRecord = META_MANAGER.getJobById("testUpdateJob");
    Assert.assertEquals(JobStatus.PENDING.name(), jobRecord.getJobStatus());
    jobRecord = JobRecord.of(
        jobRecord.getJobId(),
        0,
        JobStatus.RUNNING.name(),
        jobRecord.getJobConfig(),
        jobRecord.getAttemptTimes(),
        jobRecord.getMaxAttemptTimes(),
        jobRecord.getCTime(),
        jobRecord.getMTime(),
        jobRecord.getSTime(),
        jobRecord.getETime(),
        jobRecord.hasSubJob(),
        jobRecord.getJobInfo());
    META_MANAGER.updateJobById(jobRecord);
    jobRecord = META_MANAGER.getJobById("testUpdateJob");
    Assert.assertEquals(JobStatus.RUNNING.name(), jobRecord.getJobStatus());

    removeJob("testUpdateJob");
  }

  @Test
  public void testUpdateSubJob() {
    addSubJob("testUpdateSubJob", "S-testUpdateSubJob");
    addJob("testUpdateSubJob");

    JobRecord jobRecord = META_MANAGER.getSubJobById(
        "testUpdateSubJob", "S-testUpdateSubJob");
    Assert.assertEquals(JobStatus.PENDING.name(), jobRecord.getJobStatus());
    jobRecord = JobRecord.of(
        jobRecord.getJobId(),
        0,
        JobStatus.RUNNING.name(),
        jobRecord.getJobConfig(),
        jobRecord.getAttemptTimes(),
        jobRecord.getMaxAttemptTimes(),
        jobRecord.getCTime(),
        jobRecord.getMTime(),
        jobRecord.getSTime(),
        jobRecord.getETime(),
        jobRecord.hasSubJob(),
        jobRecord.getJobInfo());
    META_MANAGER.updateSubJobById("testUpdateSubJob", jobRecord);
    jobRecord = META_MANAGER.getSubJobById(
        "testUpdateSubJob", "S-testUpdateSubJob");
    Assert.assertEquals(JobStatus.RUNNING.name(), jobRecord.getJobStatus());

    removeSubJob("testUpdateSubJob", "S-testUpdateSubJob");
    removeJob("testUpdateSubJob");
  }

  @Test
  public void testListJobByStatus() {
    addJob("testListJobByStatus1");
    addJob("testListJobByStatus2");
    addJob("testListJobByStatus3");
    addJob("testListJobByStatus4");
    addJob("testListJobByStatus5");

    List<JobRecord> jobRecords = META_MANAGER.listJobsByStatus(JobStatus.PENDING);
    Assert.assertEquals(5, jobRecords.size());

    removeJob("testListJobByStatus1");
    removeJob("testListJobByStatus2");
    removeJob("testListJobByStatus3");
    removeJob("testListJobByStatus4");
    removeJob("testListJobByStatus5");
  }

  @Test
  public void testListJobs() {
    addJob("testListJobs1");
    addJob("testListJobs2");
    addJob("testListJobs3");
    addJob("testListJobs4");
    addJob("testListJobs5");

    List<JobRecord> jobRecords = META_MANAGER.listJobs();
    Assert.assertEquals(5, jobRecords.size());

    removeJob("testListJobs1");
    removeJob("testListJobs2");
    removeJob("testListJobs3");
    removeJob("testListJobs4");
    removeJob("testListJobs5");
  }

  @Test
  public void testListSubJobs() {
    addSubJob("testListSubJobs", "S-testListSubJobs1");
    addSubJob("testListSubJobs", "S-testListSubJobs2");
    addSubJob("testListSubJobs", "S-testListSubJobs3");
    addSubJob("testListSubJobs", "S-testListSubJobs4");
    addSubJob("testListSubJobs", "S-testListSubJobs5");
    addJob("testListSubJobs");

    List<JobRecord> jobRecords = META_MANAGER.listSubJobs("testListSubJobs");
    Assert.assertEquals(5, jobRecords.size());

    removeSubJob("testListSubJobs", "S-testListSubJobs1");
    removeSubJob("testListSubJobs", "S-testListSubJobs2");
    removeSubJob("testListSubJobs", "S-testListSubJobs3");
    removeSubJob("testListSubJobs", "S-testListSubJobs4");
    removeSubJob("testListSubJobs", "S-testListSubJobs5");
    removeJob("testListSubJobs");
  }

  @Test
  public void testListSubJobByStatus() {
    addSubJob("testListSubJobByStatus", "S-testListSubJobByStatus1");
    addSubJob("testListSubJobByStatus", "S-testListSubJobByStatus2");
    addSubJob("testListSubJobByStatus", "S-testListSubJobByStatus3");
    addSubJob("testListSubJobByStatus", "S-testListSubJobByStatus4");
    addSubJob("testListSubJobByStatus", "S-testListSubJobByStatus5");
    addJob("testListSubJobByStatus");

    List<JobRecord> jobRecords = META_MANAGER.listSubJobsByStatus(
        "testListSubJobByStatus", JobStatus.PENDING);
    Assert.assertEquals(5, jobRecords.size());

    removeSubJob("testListSubJobByStatus", "S-testListSubJobByStatus1");
    removeSubJob("testListSubJobByStatus", "S-testListSubJobByStatus2");
    removeSubJob("testListSubJobByStatus", "S-testListSubJobByStatus3");
    removeSubJob("testListSubJobByStatus", "S-testListSubJobByStatus4");
    removeSubJob("testListSubJobByStatus", "S-testListSubJobByStatus5");
    removeJob("testListSubJobByStatus");
  }

  @Test
  public void testSelectNonexistentJob() {
    JobRecord jobRecord = META_MANAGER.getJobById("nonexistent");
    Assert.assertNull(jobRecord);
  }

  private void addJob(String jobId) {
    META_MANAGER.addJob(jobId, 0,2, "job conf", true);
    assertJobCount(1, "WHERE JOB_ID='" + jobId + "'");
  }

  private void addSubJob(String parentJobId, String subJobId) {
    META_MANAGER.addSubJob(
        parentJobId,
        subJobId,
        0,
        2,
        "job conf",
        false);
    assertSubJobCount(
        parentJobId,
        1,
        "WHERE JOB_ID='" + subJobId + "'");
  }

  private void removeJob(String jobId) {
    META_MANAGER.removeJob(jobId);
    assertJobCount(0, "WHERE JOB_ID='" + jobId + "'");
  }

  private void removeSubJob(String parentJobId, String subJobId) {
    META_MANAGER.removeSubJob(parentJobId, subJobId);
    assertSubJobCount(parentJobId, 0, "WHERE JOB_ID='" + subJobId + "'");
  }

  private static void assertJobCount(int expected, String condition) {
    try (Connection conn = DriverManager.getConnection(
        MYSQL_JDBC_CONN_URL, MYSQL_JDBC_USERNAME, MYSQL_JDBC_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT COUNT(1) FROM MMA.`JOB` " + condition);
        try (ResultSet rs = stmt.getResultSet()) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(expected, rs.getInt(1));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  private static void assertSubJobCount(String parentJobId, int expected, String condition) {
    try (Connection conn = DriverManager.getConnection(
        MYSQL_JDBC_CONN_URL, MYSQL_JDBC_USERNAME, MYSQL_JDBC_PASSWORD)) {
      try (Statement stmt = conn.createStatement()) {
        stmt.executeQuery("SELECT COUNT(1) FROM MMA." + parentJobId + " " + condition);
        try (ResultSet rs = stmt.getResultSet()) {
          Assert.assertTrue(rs.next());
          Assert.assertEquals(expected, rs.getInt(1));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
