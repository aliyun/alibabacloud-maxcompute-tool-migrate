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

public class WebUITab {
  private WebUI parent;
  private List<WebUIPage> pages = new LinkedList<>();
  private String prefix;

  public WebUITab(WebUI parent, String prefix) {
    this.parent = parent;
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getBasePath() {
    return parent.getBasePath();
  }

  public List<WebUITab> getHeaderTabs() {
    return parent.getTabs();
  }

  public List<WebUIPage> getPages() {
    return pages;
  }

  public void attachPage(WebUIPage page) {
    page.setPrefix(prefix + "/" + page.getPrefix());
    pages.add(page);
  }
}
