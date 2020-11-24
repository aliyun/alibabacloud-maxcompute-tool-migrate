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

package com.aliyun.odps.datacarrier.taskscheduler.task;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

public class DropTemporaryTableTask extends AbstractTask {

  private String db;
  private String tbl;

  public DropTemporaryTableTask(String id, DirectedAcyclicGraph<Action, DefaultEdge> dag, MmaMetaManager mmaMetaManager,
                                String db, String tbl) {
    super(id, dag, mmaMetaManager);
    this.db = db;
    this.tbl = tbl;
  }

  @Override
  void updateMetadata() throws MmaException {
    if (TaskProgress.SUCCEEDED.equals(progress)) {
      mmaMetaManager.removeTemporaryTableMeta(getOriginId(), db, tbl);
    }
  }
}
