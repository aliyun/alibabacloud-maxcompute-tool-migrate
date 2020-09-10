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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.aliyun.odps.datacarrier.taskscheduler.MmaConfig.Config;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventSenderType;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventType;

public class MmaEventConfig implements Config {

  public static class MmaEventSenderConfig {
    private MmaEventSenderType type;
    private String webhookUrl;

    public MmaEventSenderConfig(MmaEventSenderType type) {
      this.type = Objects.requireNonNull(type);
    }

    public void setWebhookUrl(String webhookUrl) {
      this.webhookUrl = webhookUrl;
    }

    public MmaEventSenderType getType() {
      return type;
    }

    public String getWebhookUrl() {
      return webhookUrl;
    }
  }

  private List<MmaEventType> blacklist;
  private List<MmaEventType> whitelist;
  private List<MmaEventSenderConfig> eventSenderConfigs;

  public MmaEventConfig() {
    blacklist = new LinkedList<>();
    whitelist = new LinkedList<>();
    eventSenderConfigs = new LinkedList<>();
  }

  public void setBlacklist(List<MmaEventType> blacklist) {
    this.blacklist = blacklist;
  }

  public void setWhitelist(List<MmaEventType> whitelist) {
    this.whitelist = whitelist;
  }

  public void setEventSenderConfigs(List<MmaEventSenderConfig> eventSenderConfigs) {
    this.eventSenderConfigs = eventSenderConfigs;
  }

  public List<MmaEventType> getBlacklist() {
    return blacklist;
  }

  public List<MmaEventType> getWhitelist() {
    return whitelist;
  }

  public List<MmaEventSenderConfig> getEventSenderConfigs() {
    return eventSenderConfigs;
  }

  @Override
  public boolean validate() {
    return true;
  }
}
