package com.aliyun.odps.mma.server.action;

import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;

public class ActionExecutionContext {

  private JobConfiguration config;

  private List<List<Object>> sourceVerificationResult;
  private List<List<Object>> destVerificationResult;

  public ActionExecutionContext(JobConfiguration config) {
    this.sourceVerificationResult = null;
    this.destVerificationResult = null;
    this.config = config;
  }

  public JobConfiguration getConfig() {
    return config;
  }

  public List<List<Object>> getSourceVerificationResult() {
    return sourceVerificationResult;
  }

  public void setSourceVerificationResult(List<List<Object>> rows) {
    sourceVerificationResult = rows;
  }

  public List<List<Object>> getDestVerificationResult() {
    return destVerificationResult;
  }

  public void setDestVerificationResult(List<List<Object>> rows) {
    destVerificationResult = rows;
  }
}