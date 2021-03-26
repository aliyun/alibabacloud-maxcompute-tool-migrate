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

package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.OdpsSqlUtils;


public class OdpsDestVerificationAction extends OdpsSqlAction {
  private static final Logger LOG = LogManager.getLogger(OdpsDestVerificationAction.class);

  public OdpsDestVerificationAction(String id) {
    super(id);
  }

  @Override
  String getSql() {
    return OdpsSqlUtils.getVerifySql(actionExecutionContext.getTableMetaModel());
  }

  @Override
  Map<String, String> getSettings() {
    return actionExecutionContext
        .getOdpsConfig()
        .getDestinationTableSettings()
        .getVerifySettings();
  }

  @Override
  public void afterExecution() throws MmaException {
    try {
      List<List<String>> rows = (List) future.get();

      if (Level.DEBUG.equals(LOG.getLevel())) {
        for (List<String> row : rows) {
          LOG.debug("Dest verification result: {}", row);
        }
      }

      actionExecutionContext.setDestVerificationResult(rows);
      setProgress(ActionProgress.SUCCEEDED);
    } catch (Exception e) {
      LOG.error("Action failed, actionId: {}, stack trace: {}",
                id, ExceptionUtils.getFullStackTrace(e));
      setProgress(ActionProgress.FAILED);
    }
  }

  @Override
  public String getName() {
    return "Dest verification";
  }
}
