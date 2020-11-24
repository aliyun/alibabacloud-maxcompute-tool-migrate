package com.aliyun.odps.datacarrier.taskscheduler.action;


import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;

public interface Action {

  String getId();

  String getName();

  ActionProgress getProgress();

  Long getStartTime();

  Long getEndTime();

  AbstractActionInfo getActionInfo();

  boolean tryAllocateResource();

  void releaseResource();

  void execute() throws MmaException;

  void afterExecution() throws MmaException;

  boolean executionFinished();

  void stop();
}
