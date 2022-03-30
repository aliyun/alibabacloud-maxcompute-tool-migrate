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

import java.util.List;
import java.util.Properties;

import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import com.aliyun.odps.mma.server.meta.generated.JobDao;

public class MetaManager {

  private static final Logger LOG = LogManager.getLogger(MetaManager.class);

  private static final String MYBATIS_CONFIG = "mybatis-config.xml";

  private SqlSessionFactory sqlSessionFactory;

  public MetaManager() {
    initSqlSessionFactory();
    setUp();
  }

  public SqlSessionFactory getSqlSessionFactory() {
    return sqlSessionFactory;
  }

  private void setUp() {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao jobMapper = session.getMapper(JobDao.class);
      jobMapper.createMmaSchemaIfNotExists();
      jobMapper.createJobTableIfNotExists();
    }
  }

  private void initSqlSessionFactory() {
    Properties properties = new Properties();
    properties.put(
        "url",
        MmaServerConfiguration.getInstance().get(MmaServerConfiguration.META_DB_JDBC_URL));
    properties.put(
        "username",
        MmaServerConfiguration.getInstance().get(MmaServerConfiguration.META_DB_JDBC_USERNAME));
    properties.put(
        "password",
        MmaServerConfiguration.getInstance().get(MmaServerConfiguration.META_DB_JDBC_PASSWORD));
    sqlSessionFactory = new SqlSessionFactoryBuilder().build(
        this.getClass().getClassLoader().getResourceAsStream(MYBATIS_CONFIG),
        MmaServerConfiguration.getInstance().get(MmaServerConfiguration.META_DB_TYPE),
        properties);
  }

  public void addJob(
      String jobId,
      int jobPriority,
      int maxAttemptTimes,
      String jobConf,
      boolean hasSubJob) {
    long time = System.currentTimeMillis();
    JobRecord record = JobRecord.of(
        jobId,
        jobPriority,
        JobStatus.PENDING.name(),
        jobConf,
        // TODO: use a constant
        0,
        maxAttemptTimes,
        time,
        time,
        -1,
        -1,
        hasSubJob,
        null);
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.insertJob(record);
      if (hasSubJob) {
        dao.createSubJobTableIfNotExists(jobId);
      }
    }
  }

  public void addSubJob(
      String parentJobId,
      String subJobId,
      int jobPriority,
      int maxAttemptTimes,
      String jobConf,
      boolean hasSubJob) {
    long time = System.currentTimeMillis();
    JobRecord record = JobRecord.of(
        subJobId,
        jobPriority,
        JobStatus.PENDING.name(),
        jobConf,
        // TODO: use a constant
        0,
        maxAttemptTimes,
        time,
        time,
        -1,
        -1,
        hasSubJob,
        null);
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.createSubJobTableIfNotExists(parentJobId);
      dao.insertSubJob(parentJobId, record);
    }
  }

  public void addSubJob(
          String parentJobId,
          String subJobId,
          int jobPriority,
          int maxAttemptTimes,
          String jobConf,
          boolean hasSubJob,
          SqlSession session) {
    long time = System.currentTimeMillis();
    JobRecord record = JobRecord.of(
            subJobId,
            jobPriority,
            JobStatus.PENDING.name(),
            jobConf,
            // TODO: use a constant
            0,
            maxAttemptTimes,
            time,
            time,
            -1,
            -1,
            hasSubJob,
            null);

    JobDao dao = session.getMapper(JobDao.class);
    dao.createSubJobTableIfNotExists(parentJobId);
    dao.insertSubJob(parentJobId, record);
  }

  public void removeJob(String jobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.deleteJobById(jobId);
      // This table may not exist
      dao.dropSubJobTable(jobId);
    }
  }

  public void removeSubJob(String parentJobId, String subJobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.deleteSubJobById(parentJobId, subJobId);
      // This table may not exist
      dao.dropSubJobTable(subJobId);
    }
  }

  //TODO refactor
  public void removeSubJobTable(String parentJobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.dropSubJobTable(parentJobId);
    }
  }

  public void createSubJobTable(String parentJobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.createSubJobTableIfNotExists(parentJobId);
    }
  }

  public JobRecord getJobById(String jobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      return dao.selectJobById(jobId);
    }
  }

  public JobRecord getSubJobById(String parentJobId, String subJobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      return dao.selectSubJobById(parentJobId, subJobId);
    }
  }

  public void updateJobById(JobRecord record) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.updateJobById(record);
    }
  }

  public void updateSubJobById(String parentJobId, JobRecord record) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.updateSubJobById(parentJobId, record);
    }
  }

  public List<JobRecord> listJobs() {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      return dao.selectJobs();
    }
  }

  public List<JobRecord> listSubJobs(String parentJobId) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.createSubJobTableIfNotExists(parentJobId);
      return dao.selectSubJobs(parentJobId);
    }
  }

  public List<JobRecord> listJobsByStatus(JobStatus jobStatus) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      return dao.selectJobsByJobStatus(jobStatus.name());
    }
  }

  public List<JobRecord> listSubJobsByStatus(String parentJobId, JobStatus jobStatus) {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      JobDao dao = session.getMapper(JobDao.class);
      dao.createSubJobTableIfNotExists(parentJobId);
      return dao.selectSubJobsByJobStatus(parentJobId, jobStatus.name());
    }
  }
}
