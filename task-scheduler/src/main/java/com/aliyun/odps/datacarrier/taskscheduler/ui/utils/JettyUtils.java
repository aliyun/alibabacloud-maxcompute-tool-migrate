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

package com.aliyun.odps.datacarrier.taskscheduler.ui.utils;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUIPage;
import com.aliyun.odps.datacarrier.taskscheduler.ui.api.AbstractRestfulApi;

public class JettyUtils {
  private static final Logger LOG = LogManager.getLogger(JettyUtils.class);

  public static class ServerInfo {
    private Server server;
    private int port;
    private ContextHandlerCollection collection;

    public ServerInfo(Server server, int port, ContextHandlerCollection collection) {
      this.server = server;
      this.port = port;
      this.collection = collection;
    }

    public void addHandler(ContextHandler handler) throws Exception {
      collection.addHandler(handler);
      if (!handler.isStarted()) {
        handler.start();
      }
    }

    public void removeHandler(ContextHandler handler) throws Exception {
      collection.removeHandler(handler);
      if (handler.isStarted()) {
        handler.stop();
      }
    }

    public void stop() throws Exception {
      server.stop();
    }
  }

  public static HttpServlet createServlet(WebUIPage page) {
    return new HttpServlet() {
      @Override
      protected void doGet(HttpServletRequest req, HttpServletResponse resp)
          throws ServletException, IOException {
        resp.setContentType(String.format("%s;charset=utf-8", "text/html"));
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().print(page.render(req));
      }
    };
  }

  public static HttpServlet createServlet(final AbstractRestfulApi api) {
    return new HttpServlet() {
      @Override
      protected void doGet(
          HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType(String.format("%s;charset=utf-8", new Object[] { "application/json" }));
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().print(api.handleGet(req));
      }

      @Override
      protected void doPost(
          HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType(String.format("%s;charset=utf-8", new Object[] { "application/json" }));
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().print(api.handlePost(req));
      }

      @Override
      protected void doDelete(
          HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType(String.format("%s;charset=utf-8", new Object[] { "application/json" }));
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().print(api.handleDelete(req));
      }
    };
  }

  public static ServletContextHandler createServletHandler(
      String path,
      WebUIPage page,
      String basePath) {
    String prefixedPath;
    if ("".equals(basePath) && "/".equals(path)) {
      prefixedPath = path;
    } else {
      prefixedPath = basePath + path;
    }

    ServletContextHandler contextHandler = new ServletContextHandler();
    ServletHolder holder = new ServletHolder(createServlet(page));
    contextHandler.setContextPath(prefixedPath);
    contextHandler.addServlet(holder, "/");
    return contextHandler;
  }

  public static ServletContextHandler createServletHandler(
      String path,
      HttpServlet servlet,
      String basePath) {
    String prefixedPath;
    if ("".equals(basePath) && "/".equals(path)) {
      prefixedPath = path;
    } else {
      prefixedPath = basePath + path;
    }

    ServletContextHandler contextHandler = new ServletContextHandler();
    ServletHolder holder = new ServletHolder(servlet);
    contextHandler.setContextPath(prefixedPath);
    contextHandler.addServlet(holder, "/");
    return contextHandler;
  }

  public static ServletContextHandler createStaticHandler(String resourceBase, String path) {
    ServletContextHandler contextHandler = new ServletContextHandler();
    contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false");
    DefaultServlet staticHandler = new DefaultServlet();
    ServletHolder holder = new ServletHolder(staticHandler);
    URL res = JettyUtils.class.getClassLoader().getResource(resourceBase);
    if (res != null) {
      holder.setInitParameter("resourceBase", res.toString());
    } else {
      throw new RuntimeException("Could not find resource path for Web UI: " + resourceBase);
    }
    contextHandler.setContextPath(path);
    contextHandler.addServlet(holder, "/");
    return contextHandler;
  }


  public static ServletContextHandler createRedirectHandler(
      String srcPath,
      String destPath,
      String basePath) {
    String prefixedDestPath = basePath + destPath;

    HttpServlet httpServlet = new HttpServlet() {
      @Override
      protected void doGet(
          HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        LOG.debug("Enter redirect handler");
        LOG.debug("HTTP method: {}", req.getMethod());
        LOG.debug("Request URI: {}", req.getRequestURI());
        doRequest(req, resp);
      }

      private void doRequest(
          HttpServletRequest request, HttpServletResponse response) throws IOException {
        String newUrl =
            new URL(new URL(request.getRequestURL().toString()), prefixedDestPath).toString();
        response.sendRedirect(newUrl);
      }
    };

    return createServletHandler(srcPath, httpServlet, basePath);
  }

  public static ServerInfo startJettyServer(
      String host,
      int port,
      int maxThreads,
      int minThreads,
      List<ServletContextHandler> handlers) {
    QueuedThreadPool pool = new QueuedThreadPool(maxThreads, minThreads);
    pool.setDaemon(true);
    Server server = new Server(pool);
    ContextHandlerCollection collection = new ContextHandlerCollection();
    for (ServletContextHandler handler : handlers) {
      LOG.debug("Found available context path: {}", handler.getContextPath());
      collection.addHandler(handler);
    }
    server.setHandler(collection);

    try {
      ServerConnector connector = new ServerConnector(server);
      connector.setPort(port);
      connector.setHost(host);
      server.addConnector(connector);

      server.start();

      LOG.info("MMA UI started at: {}", server.getURI());
    } catch (Exception e) {
      e.printStackTrace();
    }

    return new ServerInfo(server, port, collection);
  }
}
