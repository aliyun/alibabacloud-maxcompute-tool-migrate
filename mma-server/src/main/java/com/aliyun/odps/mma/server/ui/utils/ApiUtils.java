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

package com.aliyun.odps.mma.server.ui.utils;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.jetty.http.MimeTypes;

import com.aliyun.odps.mma.config.ErrorOutputsV1;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.util.GsonUtils;

public class ApiUtils {
  public static void handleError(HttpServletResponse resp, int sc, String message)
      throws IOException {
    resp.setStatus(sc);
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
    resp.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
    Writer writer = resp.getWriter();

    OutputsWrapper<ErrorOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    ErrorOutputsV1 outputs = new ErrorOutputsV1(message, Collections.emptyList());
    wrapper.setOutputs(outputs);
    writer.write(GsonUtils.GSON.toJson(wrapper));
  }

  public static void handleError(HttpServletResponse resp, int sc, Throwable th)
      throws IOException {
    resp.setStatus(sc);
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
    resp.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
    Writer writer = resp.getWriter();

    OutputsWrapper<ErrorOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    ErrorOutputsV1 outputs = new ErrorOutputsV1(th.getMessage(), Arrays
        .asList(ExceptionUtils.getStackFrames(th)));
    wrapper.setOutputs(outputs);
    writer.write(GsonUtils.GSON.toJson(wrapper));
  }
}
