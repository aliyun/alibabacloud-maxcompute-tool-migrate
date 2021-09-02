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

package com.aliyun.odps.mma.server.ui;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.eclipse.jetty.servlet.ServletContextHandler;

import com.aliyun.odps.mma.server.ui.utils.JettyUtils;

public abstract class WebUi {

  private String basePath;
  private JettyUtils.ServerInfo serverInfo;
  private List<WebUiTab> tabs;
  private List<ServletContextHandler> handlers;

  public WebUi(String basePath) {
    this.basePath = basePath;
    tabs = new LinkedList<>();
    handlers = new LinkedList<>();
    basePath = Objects.requireNonNull(basePath);
  }


  public String getBasePath() {
    return basePath;
  }

  public List<WebUiTab> getTabs() {
    return tabs;
  }

  public List<ServletContextHandler> getHandlers() {
    return handlers;
  }

  public void attachHandler(ServletContextHandler handler) {
    handlers.add(handler);
  }

  public void attachTab(WebUiTab tab) {
    tab.getPages().forEach(this::attachPage);
    tabs.add(tab);
  }

  public void attachPage(WebUiPage page) {
    String pagePath = "/" + page.getPrefix();
    ServletContextHandler handler = JettyUtils.createServletHandler(pagePath, page, basePath);
    handlers.add(handler);
  }

  public void addStaticHandler(String resourceBase, String path) {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path));
  }

  public void bind(String host, int port, int maxThreads, int minThreads) {
    List<ServletContextHandler> allHandlers = new LinkedList<>(handlers);
    serverInfo = JettyUtils.startJettyServer(host, port, maxThreads, minThreads, allHandlers);
  }

  public void stop() throws Exception {
    if (serverInfo != null) {
      serverInfo.stop();
    }
  }
}
