package com.aliyun.odps.datacarrier.taskscheduler.task;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import java.util.List;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

public interface Task {
  TaskProgress getProgress();

  List<Action> getExecutableActions();

  DirectedAcyclicGraph<Action, DefaultEdge> getDag();

  String getJobId();

  String getId();

  Long getStartTime();

  Long getEndTime();

  void stop() throws MmaException;
}
