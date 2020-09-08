package com.aliyun.odps.datacarrier.taskscheduler.action;


import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;

public interface Action {

  String getId();

  ActionProgress getProgress();

  AbstractActionInfo getActionInfo();

  boolean tryAllocateResource();

  void releaseResource();

  void execute() throws MmaException;

  void afterExecution() throws MmaException;

  boolean executionFinished();

  void stop();
}
