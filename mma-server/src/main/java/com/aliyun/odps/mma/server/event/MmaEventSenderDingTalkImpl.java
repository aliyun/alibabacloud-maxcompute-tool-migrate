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

package com.aliyun.odps.mma.server.event;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiRobotSendRequest;

public class MmaEventSenderDingTalkImpl implements MmaEventSender {
  private static final Logger LOG = LogManager.getLogger(MmaEventSenderDingTalkImpl.class);

  private DingTalkClient dingTalkClient;

  public MmaEventSenderDingTalkImpl(String webhookUrl) {
    dingTalkClient = new DefaultDingTalkClient(Objects.requireNonNull(webhookUrl));
  }

  @Override
  public void send(BaseMmaEvent event) {
    OapiRobotSendRequest request = new OapiRobotSendRequest();
    request.setMsgtype("markdown");
    OapiRobotSendRequest.Markdown markdown = new OapiRobotSendRequest.Markdown();
    markdown.setTitle(event.getType().name());
    markdown.setText(event.toString());
    request.setMarkdown(markdown);
    try {
      dingTalkClient.execute(request);
      LOG.info("Event sent, id: {}", event.getId());
    } catch (Exception e) {
      LOG.warn("Failed to send dingtalk message", e);
    }
  }
}
