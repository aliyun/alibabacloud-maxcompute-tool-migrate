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


package com.aliyun.odps.mma.server.task;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.server.action.Action;
import com.aliyun.odps.mma.server.action.ActionProgress;

public enum TaskProgress {
  /**
   * Change to {@link #RUNNING} when actionStatus is {@link ActionProgress#RUNNING}.
   */
  PENDING {
    @Override
    TaskProgress next(DirectedAcyclicGraph<Action, DefaultEdge> dag, ActionProgress actionStatus) {
      switch (actionStatus) {
        case RUNNING:
          return RUNNING;
        default:
          throw new IllegalStateException("Invalid status change");
      }
    }
  },
  /**
   * Change to {@link #SUCCEEDED} when actionStatus is @{@link ActionProgress#SUCCEEDED} and
   */
  RUNNING {
    @Override
    TaskProgress next(DirectedAcyclicGraph<Action, DefaultEdge> dag, ActionProgress actionStatus) {
      switch (actionStatus) {
        case SUCCEEDED:
          boolean allActionsSucceeded = dag == null || dag.vertexSet().stream().allMatch(v -> ActionProgress.SUCCEEDED.equals(v.getProgress()));
          if (allActionsSucceeded) {
            return SUCCEEDED;
          } else {
            return RUNNING;
          }
        case RUNNING:
          return RUNNING;
        case FAILED:
          return FAILED;
        case CANCELED:
          return CANCELED;
        default:
          throw new IllegalStateException("Invalid status change");
      }
    }
  },
  /**
   * Final status.
   */
  SUCCEEDED {
    @Override
    TaskProgress next(DirectedAcyclicGraph<Action, DefaultEdge> dag, ActionProgress actionStatus) {
      throw new IllegalStateException("Invalid status change from SUCCEEDED");
    }
  },
  /**
   * Final status.
   */
  FAILED {
    @Override
    TaskProgress next(DirectedAcyclicGraph<Action, DefaultEdge> dag, ActionProgress actionStatus) {
      throw new IllegalStateException("Invalid status change from FAILED");
    }
  },
  /**
   * Final status.
   */
  CANCELED {
    @Override
    TaskProgress next(DirectedAcyclicGraph<Action, DefaultEdge> dag, ActionProgress actionStatus) {
      throw new IllegalStateException("Invalid status change from CANCELED");
    }
  };

  abstract TaskProgress next(
      DirectedAcyclicGraph<Action, DefaultEdge> dag, ActionProgress actionStatus);
}
