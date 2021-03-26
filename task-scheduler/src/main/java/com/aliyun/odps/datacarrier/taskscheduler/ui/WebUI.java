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

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.aliyun.odps.datacarrier.taskscheduler.ui.api.AbstractRestfulApi;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.JettyUtils;

public abstract class WebUI {

  private String basePath;
  private JettyUtils.ServerInfo serverInfo;
  private List<WebUITab> tabs;
  private List<ServletContextHandler> handlers;
  private ServletContextHandler apiHandler;

  public WebUI(String basePath) {
    this.basePath = basePath;
    tabs = new LinkedList();
    handlers = new LinkedList();
    basePath = Objects.requireNonNull(basePath);
    apiHandler = new ServletContextHandler();

    String apiPath;
    if ("".equals(basePath)) {
      apiPath = "/api";
    } else {
      apiPath = basePath + "/api";
    }
    this.apiHandler.setContextPath(apiPath);
  }


  public String getBasePath() {
    return basePath;
  }

  public List<WebUITab> getTabs() {
    return tabs;
  }

  public List<ServletContextHandler> getHandlers() {
    return handlers;
  }

  public void attachHandler(ServletContextHandler handler) {
    handlers.add(handler);
  }

  public void attachTab(WebUITab tab) {
    tab.getPages().forEach(this::attachPage);
    tabs.add(tab);
  }

  public void attachPage(WebUIPage page) {
    String pagePath = "/" + page.getPrefix();
    ServletContextHandler handler = JettyUtils.createServletHandler(pagePath, page, basePath);
    handlers.add(handler);
  }

  public void attachApi(AbstractRestfulApi api) {
    ServletHolder holder = new ServletHolder(JettyUtils.createServlet(api));
    apiHandler.addServlet(holder, api.getPrefix());
  }

  public void addStaticHandler(String resourceBase, String path) {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path));
  }

  public void setApiErrorHandler(ErrorHandler errorHandler) {
    apiHandler.setErrorHandler(errorHandler);
  }

  public void bind(String host, int port, int maxThreads, int minThreads) {
    List<ServletContextHandler> allHandlers = new LinkedList<>(handlers);
    allHandlers.add(apiHandler);
    serverInfo = JettyUtils.startJettyServer(host, port, maxThreads, minThreads, allHandlers);
  }

  public void stop() throws Exception {
    if (serverInfo != null) {
      serverInfo.stop();
    }
  }
}
