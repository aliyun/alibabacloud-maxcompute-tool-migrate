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

package com.aliyun.odps.mma.server.ui.api;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ErrorHandler;

import com.aliyun.odps.mma.config.ErrorOutputsV1;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.JsonObject;


public class ApiErrorHandler extends ErrorHandler {

  @Override
  public void handle(
      String target,
      Request baseRequest,
      HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    Throwable th = (Throwable) request.getAttribute("javax.servlet.error.exception");

    baseRequest.setHandled(true);
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
    if (th instanceof IllegalArgumentException || th instanceof IllegalStateException) {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
    } else {
      response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    Writer writer = response.getWriter();

    OutputsWrapper<ErrorOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    ErrorOutputsV1 outputs = new ErrorOutputsV1(
        th.getMessage(),
        Arrays.asList(ExceptionUtils.getStackFrames(th)));
    wrapper.setOutputs(outputs);
    writer.write(GsonUtils.GSON.toJson(wrapper));
  }
}
