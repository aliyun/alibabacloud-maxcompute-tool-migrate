/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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
 *
 */

package com.aliyun.odps.mma.meta;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.jdbc.HiveStatement;

public class HiveStatementLog {

  public static List<List<String>> setLog(Statement st) throws SQLException {
    List<List<String>> logList = new ArrayList<>();
    HiveStatement stmt = (HiveStatement) st;
    while (stmt.hasMoreLogs()) {
      List<String> logAndJobIdAndUrl = new ArrayList<>();
      for (String line : stmt.getQueryLog()) {
        logAndJobIdAndUrl.add(line);
        if (!StringUtils.isBlank(line) && line.contains("Starting Job =")) {
          String jobId = line.split("=")[1].split(",")[0];
          String trackingUrl = line.split("=")[2];
          logAndJobIdAndUrl.add(jobId);
          logAndJobIdAndUrl.add(trackingUrl);
        }
        logList.add(logAndJobIdAndUrl);
      }
    }
    return logList;
  }

}
