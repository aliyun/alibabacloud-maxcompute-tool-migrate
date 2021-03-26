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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.OdpsSqlActionInfo;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;


public class OdpsSqlExecutor extends AbstractActionExecutor {

  private static final Logger LOG = LogManager.getLogger("ExecutorLogger");

  private static class OdpsSqlCallable implements Callable<Object> {

    private Odps odps;
    private String db;
    private String sql;
    private Map<String, String> settings;
    private String actionId;
    private OdpsSqlActionInfo odpsSqlActionInfo;

    OdpsSqlCallable(
        Odps odps,
        String db,
        String sql,
        Map<String, String> settings,
        String actionId,
        OdpsSqlActionInfo odpsSqlActionInfo) {
      this.odps = odps;
      this.db = db;
      this.sql = Objects.requireNonNull(sql);
      this.settings = Objects.requireNonNull(settings);
      this.actionId = Objects.requireNonNull(actionId);
      this.odpsSqlActionInfo = Objects.requireNonNull(odpsSqlActionInfo);
    }

    @Override
    public Object call() throws Exception {
      LOG.info("ActionId: {}, Executing sql: {}, settings {}", this.actionId, this.sql,
               this.settings);

      if (this.sql.isEmpty()) {
        return null;
      }

      String project = StringUtils.isNullOrEmpty(this.db) ? this.odps.getDefaultProject() : this.db;
      Instance i = SQLTask.run(this.odps, project, this.sql, this.settings, null);

      this.odpsSqlActionInfo.setInstanceId(i.getId());
      LOG.info("ActionId: {}, InstanceId: {}", this.actionId, i.getId());

      try {
        this.odpsSqlActionInfo.setLogView(this.odps.logview().generateLogView(i, 72L));
        LOG.info("ActionId: {}, LogView {}", this.actionId, this.odpsSqlActionInfo.getLogView());
      } catch (OdpsException odpsException) {
      }

      i.waitForSuccess();

      if (OdpsSqlActionInfo.ResultType.COLUMNS.equals(this.odpsSqlActionInfo.getResultType())) {
        return parseResult(i);
      }
      List<Object> ret = new LinkedList<>();
      List<String> row = new ArrayList<>(1);
      row.add(i.getTaskResults().get("AnonymousSQLTask"));
      ret.add(row);
      return ret;
    }

    private List<Object> parseResult(Instance instance) throws OdpsException {
      List<Record> records = SQLTask.getResult(instance);
      List<Object> ret = new LinkedList<>();

      if (records.isEmpty()) {
        return ret;
      }
      int columnCount = records.get(0).getColumnCount();

      for (Record r : records) {
        List<String> row = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
          row.add(r.get(i).toString());
        }
        ret.add(row);
        LOG.debug("ActionId: {}, result: {}", this.actionId, row);
      }

      LOG.info("ActionId: {}, result set size: {}", this.actionId, ret.size());

      return ret;
    }
  }


  public Future<Object> execute(
      MmaConfig.OdpsConfig odpsConfig,
      String sql,
      Map<String, String> settings,
      String actionId,
      OdpsSqlActionInfo odpsSqlActionInfo) {
    OdpsSqlCallable callable = new OdpsSqlCallable(
        odpsConfig.toOdps(),
        odpsConfig.getProjectName(),
        sql,
        settings,
        actionId,
        odpsSqlActionInfo);

    return this.executor.submit(callable);
  }
}
