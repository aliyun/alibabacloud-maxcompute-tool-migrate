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

package com.aliyun.odps.mma.server.task;

import java.util.List;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.action.Action;

public interface Task {
  TaskProgress getProgress();

  void setStatus(Action action);

  List<Action> getExecutableActions();

  DirectedAcyclicGraph<Action, DefaultEdge> getDag();

  String getJobId();

  String getRootJobId();

  String getId();

  Long getStartTime();

  Long getEndTime();

  void stop() throws MmaException;
}
