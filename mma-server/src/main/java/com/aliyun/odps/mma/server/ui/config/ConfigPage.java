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
