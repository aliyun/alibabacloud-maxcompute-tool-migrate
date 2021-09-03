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

import com.aliyun.odps.mma.server.config.MmaEventSenderConfiguration;

public class MmaEventSenderFactory {

  public static MmaEventSender get( MmaEventSenderConfiguration config) {
    Objects.requireNonNull(config);

    switch (config.getType()) {
      case DingTalk:
        return new MmaEventSenderDingTalkImpl(config.getWebhookUrl());
      default:
        throw new IllegalArgumentException("Unsupported event sender type: " + config.getType());
    }
  }
}
