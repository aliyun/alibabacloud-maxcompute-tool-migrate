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

package com.aliyun.odps.mma.server.action.executor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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

import com.aliyun.odps.mma.server.action.info.HiveSqlActionInfo;
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

  private static class HiveSqlCallable implements Callable<List<List<Object>>> {

    private String hiveJdbcUrl;
    private String user;
    private String password;
    private String sql;
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
    public List<List<Object>> call() throws SQLException {
      LOG.info("ActionId: {}, executing sql: {}", actionId, sql);

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
                LOG.warn("ActionId: {}, fetching hive query log failed", actionId);
                break;
              }
            }
            LOG.info("ActionId: {}, no more logs", actionId);
          };
          Thread loggingThread = new Thread(logging);
          loggingThread.setDaemon(true);
          loggingThread.start();

          List<List<Object>> ret = new LinkedList<>();
          try (ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData resultSetMetaData = rs.getMetaData();
            while (rs.next()) {
              ret.add(toRow(resultSetMetaData, rs));
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

  private static List<Object> toRow(
      ResultSetMetaData resultSetMetaData,
      ResultSet rs) throws SQLException {
    List<Object> ret = new LinkedList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      int type = resultSetMetaData.getColumnType(i);
      switch (type) {
        case Types.BIT:
          ret.add(rs.getBoolean(i));
          break;
        case Types.TINYINT:
          ret.add(rs.getByte(i));
          break;
        case Types.SMALLINT:
          ret.add(rs.getShort(i));
          break;
        case Types.INTEGER:
          ret.add(rs.getInt(i));
          break;
        case Types.BIGINT:
          ret.add(rs.getLong(i));
          break;
        case Types.REAL:
          ret.add(rs.getFloat(i));
          break;
        case Types.FLOAT:
        case Types.DOUBLE:
          ret.add(rs.getDouble(i));
          break;
        case Types.DECIMAL:
        case Types.NUMERIC:
          ret.add(rs.getBigDecimal(i));
          break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          ret.add(rs.getString(i));
          break;
        case Types.DATE:
          ret.add(rs.getDate(i));
          break;
        case Types.TIME:
          ret.add(rs.getTime(i));
          break;
        case Types.TIMESTAMP:
          ret.add(rs.getTimestamp(i));
          break;
        default:
          throw new IllegalArgumentException("Unsupported JDBC type: " + type);
      }
    }

    return ret;
  }

  public Future<List<List<Object>>> execute(
      String jdbcUrl,
      String username,
      String password,
      String sql,
      Map<String, String> settings,
      String actionId,
      HiveSqlActionInfo hiveSqlActionInfo) {
    // TODO: jdbc address, user, password should come with tableMigrationConfig
    HiveSqlCallable callable = new HiveSqlCallable(
        jdbcUrl,
        username,
        password,
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

    LOG.debug("ActionId: {}, {}", actionId, log);

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
