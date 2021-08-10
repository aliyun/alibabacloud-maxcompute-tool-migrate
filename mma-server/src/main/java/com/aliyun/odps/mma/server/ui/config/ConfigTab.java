package com.aliyun.odps.mma.server.ui.config;

import com.aliyun.odps.mma.server.ui.WebUi;
import com.aliyun.odps.mma.server.ui.WebUiTab;

public class ConfigTab extends WebUiTab {

  public ConfigTab(WebUi parent, String prefix) {
    super(parent, prefix);
    attachPage(new ConfigPage("", this));
  }
}
