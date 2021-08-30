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

package com.aliyun.odps.mma.server.action.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.task.SQLTask;


public class McSqlExecutor extends AbstractActionExecutor {

  private static final Logger LOG = LogManager.getLogger("ExecutorLogger");

  private static class McSqlCallable implements Callable<List<List<Object>>> {

    private Odps odps;
    private String db;
    private String sql;
    private boolean hasResults;
    private Map<String, String> settings;
    private String actionId;
    private McSqlActionInfo mcSqlActionInfo;

    McSqlCallable(
        Odps odps,
        String db,
        String sql,
        boolean hasResults,
        Map<String, String> settings,
        String actionId,
        McSqlActionInfo mcSqlActionInfo) {
      this.odps = odps;
      this.db = db;
      this.sql = Objects.requireNonNull(sql);
      this.settings = Objects.requireNonNull(settings);
      this.actionId = Objects.requireNonNull(actionId);
      this.mcSqlActionInfo = Objects.requireNonNull(mcSqlActionInfo);
      this.hasResults = hasResults;
    }

    @Override
    public List<List<Object>> call() throws Exception {
      LOG.info("ActionId: {}, Executing sql: {}, settings {}", this.actionId, this.sql,
               this.settings);

      String project = StringUtils.isBlank(this.db) ? this.odps.getDefaultProject() : this.db;
      Instance i = SQLTask.run(this.odps, project, this.sql, this.settings, null);

      this.mcSqlActionInfo.setInstanceId(i.getId());
      LOG.info("ActionId: {}, InstanceId: {}", this.actionId, i.getId());

      try {
        this.mcSqlActionInfo.setLogView(this.odps.logview().generateLogView(i, 72L));
        LOG.info("ActionId: {}, LogView {}", this.actionId, this.mcSqlActionInfo.getLogView());
      } catch (OdpsException e) {
        LOG.warn("ActionId: {}, failed to generate logview", this.actionId);
      }

      i.waitForSuccess();
      if (hasResults) {
        return parseResult(i);
      } else {
        return Collections.emptyList();
      }
    }

    private List<List<Object>> parseResult(Instance instance) throws OdpsException, IOException {
      List<List<Object>> ret = new LinkedList<>();

      List<Record> records = SQLTask.getResultByInstanceTunnel(instance);
      if (records.isEmpty()) {
        return ret;
      }
      int columnCount = records.get(0).getColumnCount();
      for (Record r : records) {
        List<Object> row = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
          if (OdpsType.STRING.equals(r.getColumns()[i].getTypeInfo().getOdpsType())) {
            // record.get() return byte[] when type==String, use getString() to get String type
            row.add(r.getString(i));
          } else {
            row.add(r.get(i));
          }
        }
        ret.add(row);
      }
      LOG.info("ActionId: {}, result set size: {}", this.actionId, ret.size());

      return ret;
    }
  }


  public Future<List<List<Object>>> execute(
      String endpoint,
      String executeProject,
      String accessKeyId,
      String accessKeySecret,
      String sql,
      boolean hasResults,
      Map<String, String> settings,
      String actionId,
      McSqlActionInfo mcSqlActionInfo) {
    Account account = new AliyunAccount(accessKeyId, accessKeySecret);
    Odps odps = new Odps(account);
    odps.setEndpoint(endpoint);
    odps.setDefaultProject(executeProject);
    McSqlCallable callable = new McSqlCallable(
        odps,
        executeProject,
        sql,
        hasResults,
        settings,
        actionId,
        mcSqlActionInfo);

    return this.executor.submit(callable);
  }
}
