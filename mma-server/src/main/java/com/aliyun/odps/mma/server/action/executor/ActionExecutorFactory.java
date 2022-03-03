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

package com.aliyun.odps.mma.server.action.executor;


public class ActionExecutorFactory {
  private static HiveSqlExecutor hiveSqlExecutor = null;
  private static McSqlExecutor mcSqlExecutor = null;
  private static DefaultExecutor defaultExecutor = null;

  public static HiveSqlExecutor getHiveSqlExecutor() {
    if (hiveSqlExecutor == null) {
      hiveSqlExecutor = new HiveSqlExecutor();
    }

    return hiveSqlExecutor;
  }

  public static McSqlExecutor getMcSqlExecutor() {
    if (mcSqlExecutor == null) {
      mcSqlExecutor = new McSqlExecutor();
    }

    return mcSqlExecutor;
  }

  public static DefaultExecutor getDefaultExecutor() {
    if (defaultExecutor == null) {
      defaultExecutor = new DefaultExecutor();
    }

    return defaultExecutor;
  }

  public static void shutdown() {
    if (hiveSqlExecutor != null) {
      hiveSqlExecutor.shutdown();
    }

    if (mcSqlExecutor != null) {
      mcSqlExecutor.shutdown();
    }
  }
}
