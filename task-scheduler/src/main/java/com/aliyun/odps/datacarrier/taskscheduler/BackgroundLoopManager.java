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

package com.aliyun.odps.datacarrier.taskscheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BackgroundLoopManager {
  private static final Logger LOG = LogManager.getLogger(BackgroundLoopManager.class);

  private final int BACKGROUND_LOOP_INTERVAL_IN_MS = 10000;
  private Thread backgroundLoop;
  private Set<BackgroundWorkItem> items = ConcurrentHashMap.newKeySet();

  public void start() {
    startBackgroundLoop();
  }

  public boolean addWorkItem(BackgroundWorkItem item) {
    return items.add(item);
  }

  private void startBackgroundLoop() {
    LOG.info(this.getClass().getName() + " start background loop");
    this.backgroundLoop = new Thread(this.getClass().getName() + " background thread") {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          LOG.info("BackgroundWorkItem count {}", items.size());
          Iterator<BackgroundWorkItem> iter = items.iterator();
          while (iter.hasNext()) {
            BackgroundWorkItem item = iter.next();
            try {
              item.execute();
              if (item.finished()) {
                iter.remove();
              }
            } catch (Exception e) {
              LOG.error("Execute BackgroundWorkItem failed", e);
            }
          }
          try {
            Thread.sleep(BACKGROUND_LOOP_INTERVAL_IN_MS);
          } catch (InterruptedException e) {
            LOG.warn("Background loop interrupted ", e);
          }
        }
      }
    };
    this.backgroundLoop.start();
  }
}
