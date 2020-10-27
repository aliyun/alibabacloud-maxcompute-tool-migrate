/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler.action.executor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.hive.jdbc.HiveStatement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.HiveSqlActionInfo;
import com.aliyun.odps.utils.StringUtils;

public class HiveSqlExecutor extends AbstractActionExecutor {

  private static final Logger LOG = LogManager.getLogger("ExecutorLogger");

  public HiveSqlExecutor() {
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Create HiveRunner failed", e);
    }
  }

  private static class HiveSqlCallable implements Callable<List<List<String>>> {

    private String hiveJdbcUrl;
    private String user;
    private String password;
    private String sql;
    // TODO: Map<String, String> could be better
    private Map<String, String> settings;
    private String actionId;
    private HiveSqlActionInfo hiveSqlActionInfo;

    HiveSqlCallable(
        String hiveJdbcUrl,
        String user,
        String password,
        String sql,
        Map<String, String> settings,
        String actionId,
        HiveSqlActionInfo hiveSqlActionInfo) {
      this.hiveJdbcUrl = Objects.requireNonNull(hiveJdbcUrl);
      this.user = Objects.requireNonNull(user);
      this.password = Objects.requireNonNull(password);
      this.sql = Objects.requireNonNull(sql);
      this.settings = Objects.requireNonNull(settings);
      this.actionId = Objects.requireNonNull(actionId);
      this.hiveSqlActionInfo = Objects.requireNonNull(hiveSqlActionInfo);
    }

    @Override
    public List<List<String>> call() throws SQLException {
      LOG.info("ActionId: {}, Executing sql: {}", actionId, sql);

      try (Connection conn = DriverManager.getConnection(hiveJdbcUrl, user, password)) {
        try (HiveStatement stmt = (HiveStatement) conn.createStatement()) {
          settings.put("mapreduce.job.name", actionId);
          for (Entry<String, String> entry : settings.entrySet()) {
            stmt.execute("SET " + entry.getKey() + "=" + entry.getValue());
          }

          Runnable logging = () -> {
            while (stmt.hasMoreLogs()) {
              try {
                for (String line : stmt.getQueryLog()) {
                  parseLogAndSetExecutionInfo(line, actionId, hiveSqlActionInfo);
                }
              } catch (SQLException e) {
                LOG.warn("Fetching hive query log failed", e);
                break;
              }
            }
          };
          Thread loggingThread = new Thread(logging);
          loggingThread.start();

          List<List<String>> ret = new LinkedList<>();
          try (ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            while (rs.next()) {
              List<String> record = new LinkedList<>();
              for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                record.add(rs.getString(i));
              }

              LOG.debug("ActionId: {}, result: {}", actionId, record);
              ret.add(record);
            }
          }
          LOG.info("ActionId: {}, result set size: {}", actionId, ret.size());

          try {
            loggingThread.join();
          } catch (InterruptedException ignore) {
          }

          return ret;
        }
      }
    }
  }

  public Future<List<List<String>>> execute(
      String sql,
      Map<String, String> settings,
      String actionId,
      HiveSqlActionInfo hiveSqlActionInfo) {

    // TODO: jdbc address, user, password should come with tableMigrationConfig
    // TODO: different action could have different settings
    // TODO: make settings a map
    HiveSqlCallable callable = new HiveSqlCallable(
        MmaServerConfig.getInstance().getHiveConfig().getJdbcConnectionUrl(),
        MmaServerConfig.getInstance().getHiveConfig().getUser(),
        MmaServerConfig.getInstance().getHiveConfig().getPassword(),
        sql,
        settings,
        actionId,
        hiveSqlActionInfo);

    return executor.submit(callable);
  }

  private static void parseLogAndSetExecutionInfo(
      String log,
      String actionId,
      HiveSqlActionInfo hiveSqlActionInfo) {

    if (StringUtils.isNullOrEmpty(log)) {
      return;
    }
    if (!log.contains("Starting Job =")) {
      return;
    }
    String jobId = log.split("=")[1].split(",")[0];
    String trackingUrl = log.split("=")[2];
    hiveSqlActionInfo.setJobId(jobId);
    hiveSqlActionInfo.setTrackingUrl(trackingUrl);
    LOG.info("ActionId: {}, jobId: {}", actionId, jobId);
    LOG.info("ActionId: {}, tracking url: {}", actionId, trackingUrl);
  }
}
