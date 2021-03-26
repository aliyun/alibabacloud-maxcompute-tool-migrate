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

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.ErrorHandler;

import com.google.gson.JsonObject;


public class ApiErrorHandler extends ErrorHandler {

  private static final Logger LOG = LogManager.getLogger(com.aliyun.odps.datacarrier.taskscheduler.ui.MmaUI.class);

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
    Writer writer = response.getWriter();

    JsonObject root = new JsonObject();
    root.addProperty("ProtocolVersion", 1);
    JsonObject output = new JsonObject();
    output.addProperty("ErrorMessage", th.getMessage());
    root.add("Output", output);

    writer.write(root.toString());
  }
}
