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

package com.aliyun.odps.datacarrier.taskscheduler.ui.api;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.net.util.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.config.OutputsWrapper;
import com.aliyun.odps.datacarrier.taskscheduler.config.PublicKeyOutputsV1;

public class PublicKeyApi extends AbstractRestfulApi
{
  private static final Logger LOG = LogManager.getLogger(PublicKeyApi.class);

  public PublicKeyApi(String prefix) throws MmaException { super(prefix); }

  @Override
  public String handleGet(HttpServletRequest request) throws ServletException {
    return getPublicKeyOutputsV1();
  }

  private String getPublicKeyOutputsV1() throws ServletException {
    OutputsWrapper<PublicKeyOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    PublicKeyOutputsV1 outputs = new PublicKeyOutputsV1();
    outputs.setMmaSecurityEnabled(isSecurityEnabled());
    if (isSecurityEnabled()) {
      outputs.setPublicKey(Base64.encodeBase64String(this.publicKey.getEncoded()));
    } else {
      throw new ServletException("MMA security is disabled.");
    }
    wrapper.setOutputs(outputs);
    return GSON.toJson(wrapper);
  }
}
