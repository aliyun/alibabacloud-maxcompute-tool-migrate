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

package com.aliyun.odps.datacarrier.taskscheduler.ui;

import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.AbstractRestfulApi;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.ApiErrorHandler;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.JobApi;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.PublicKeyApi;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.JettyUtils;

public class MmaApi {

  private JettyUtils.ServerInfo serverInfo;
  private ServletContextHandler apiHandler;

  public MmaApi(
      String basePath,
      MmaMetaManager mmaMetaManager,
      TaskScheduler taskScheduler) throws MmaException {

    apiHandler = new ServletContextHandler();

    String apiPath;
    if ("".equals(basePath)) {
      apiPath = "/api";
    } else {
      apiPath = basePath + "/api";
    }
    this.apiHandler.setContextPath(apiPath);

    attachApi(new JobApi("/jobs", mmaMetaManager, taskScheduler));
    attachApi(new PublicKeyApi("/publickey"));
    setApiErrorHandler(new ApiErrorHandler());
  }

  public void attachApi(AbstractRestfulApi api) {
    ServletHolder holder = new ServletHolder(JettyUtils.createServlet(api));
    apiHandler.addServlet(holder, api.getPrefix());
  }

  public void setApiErrorHandler(ErrorHandler errorHandler) {
    apiHandler.setErrorHandler(errorHandler);
  }

  public void bind(String host, int port, int maxThreads, int minThreads) {
    serverInfo = JettyUtils.startJettyServer(host, port, maxThreads, minThreads, apiHandler);
  }

  public void stop() throws Exception {
    if (serverInfo != null) {
      serverInfo.stop();
    }
  }
}
