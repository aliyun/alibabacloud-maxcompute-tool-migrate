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

package com.aliyun.odps.mma.server.ui.config;

import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.server.ui.WebUiPage;
import com.aliyun.odps.mma.server.ui.WebUiTab;
import com.aliyun.odps.mma.server.ui.utils.UiUtils;
import j2html.tags.DomContent;

public class ConfigPage extends WebUiPage {
  private static final Logger LOG = LogManager.getLogger(ConfigPage.class);

  private WebUiTab parent;

  public ConfigPage(String prefix, WebUiTab parent) {
    super(prefix);
    this.parent = parent;
  }

  @Override
  public String render(HttpServletRequest request) {
    List<DomContent> content = new LinkedList<>();

    String parameterPath = String.join("/", parent.getBasePath(), parent.getPrefix());
    content.add(
        UiUtils.configTable(
            parameterPath,
            request,
            "config",
            "config",
            LOG)
    );
    return UiUtils.basicMmaPage("MMA Server Configuration", content, parent);
  }
}
