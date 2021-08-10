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

package com.aliyun.odps.mma.server.action.executor;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public abstract class AbstractActionExecutor implements ActionExecutor {
  private static final Logger LOG = LogManager.getLogger("ExecutorLogger");

  private static final int DEFAULT_CORE_POOL_SIZE = 20;
  private static final int DEFAULT_MAX_POOL_SIZE = 50;
  private static final long DEFAULT_KEEP_ALIVE_SECONDS = 60L;

  ThreadPoolExecutor executor;

  public AbstractActionExecutor() {
    ThreadFactory factory = (new ThreadFactoryBuilder())
        .setNameFormat("ActionExecutor- #%d")
        .setDaemon(true)
        .build();

    this.executor = new ThreadPoolExecutor(
        DEFAULT_CORE_POOL_SIZE,
        DEFAULT_MAX_POOL_SIZE,
        DEFAULT_KEEP_ALIVE_SECONDS,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue(),
        factory,
        (r, executor) -> LOG.warn("Failed to submit task to ThreadPoolExecutor:" + executor));
  }

  @Override
  public void shutdown() { this.executor.shutdown(); }
}
