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

import java.util.HashMap;
import java.util.Map;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.server.action.Action;

public class ActionGraph {

  DirectedAcyclicGraph<Action, DefaultEdge> dag;

  public ActionGraph(DirectedAcyclicGraph<Action, DefaultEdge> dag) {
    this.dag = dag;
  }

  public String toDotFile() {
    StringBuilder dotFileBuilder = new StringBuilder();
    dotFileBuilder.append("digraph G {\n");
    dotFileBuilder.append("  subgraph task {\n");
    int index = 0;
    Map<Action, Integer> actionToIndex = new HashMap<>();

    for (Action action : dag.vertexSet()) {
      dotFileBuilder.append(toDotNode(action, index));
      actionToIndex.put(action, index);
      index += 1;
    }

    dotFileBuilder.append("  }\n");

    for (Action action : dag.vertexSet()) {
      for (Action ancestor : Graphs.predecessorListOf(dag, action)) {
        dotFileBuilder.append(String.format("  %d->%d;\n",
                                            actionToIndex.get(ancestor),
                                            actionToIndex.get(action)));
      }
    }

    dotFileBuilder.append("}\n");

    return dotFileBuilder.toString();
  }

  public String toDotNode(Action action, int index) {
    return String.format("    %d [label=\"%s\" class=\"%s\"]\n",
                         index,
                         action.getName(),
                         action.getProgress().name().toLowerCase());
  }
}
