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

package com.aliyun.odps.mma.server.ui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;
// http://devdoc.net/javaweb/jetty-9.2.6.v20141205-javadoc/org/eclipse/jetty/http/HttpMethod.html
// use jetty9 method
import org.eclipse.jetty.http.HttpMethod;

public class MmaJettyServer extends Server {

  public MmaJettyServer(ThreadPool pool) {
    super(pool);
  }

  /**
   * Override handle to disable TRACE
   */
  @Override
  public void handle(HttpChannel connection) throws IOException, ServletException {
    Request request=connection.getRequest();
    Response response=connection.getResponse();

    if (HttpMethod.TRACE.name().equals(request.getMethod())){
      request.setHandled(true);
      response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    } else {
      super.handle(connection);
    }
  }
}
