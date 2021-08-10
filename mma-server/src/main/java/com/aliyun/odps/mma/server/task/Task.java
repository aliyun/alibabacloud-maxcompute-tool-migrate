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
