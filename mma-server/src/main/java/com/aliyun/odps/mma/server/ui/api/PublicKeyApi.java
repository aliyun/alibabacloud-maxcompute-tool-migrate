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

package com.aliyun.odps.mma.server.ui.api;

import java.io.IOException;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.net.util.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.ErrorOutputsV1;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.config.PublicKeyOutputsV1;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.util.GsonUtils;

public class PublicKeyApi extends AbstractRestfulApi {

  public PublicKeyApi(String prefix) throws MmaException {
    super(prefix);
  }

  @Override
  public void handleGet(
      HttpServletRequest request,
      HttpServletResponse resp) throws IOException {

    OutputsWrapper<PublicKeyOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    PublicKeyOutputsV1 outputs;
    if (isSecurityEnabled()) {
      outputs = new PublicKeyOutputsV1(
          isSecurityEnabled(),
          Base64.encodeBase64String(this.publicKey.getEncoded()));
    } else {
      outputs = new PublicKeyOutputsV1(isSecurityEnabled(), null);
    }
    wrapper.setOutputs(outputs);
    resp.getWriter().print(GsonUtils.GSON.toJson(wrapper));
    resp.setStatus(HttpServletResponse.SC_OK);
  }
}
