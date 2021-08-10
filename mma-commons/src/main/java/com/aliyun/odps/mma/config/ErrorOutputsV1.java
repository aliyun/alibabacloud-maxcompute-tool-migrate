package com.aliyun.odps.mma.config;

import java.util.List;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class ErrorOutputsV1 {
  @Expose
  @SerializedName("ErrorMessage")
  String errorMessage;

  @Expose
  @SerializedName("StackTrace")
  List<String> stackTrace;

  public ErrorOutputsV1(String errorMessage, List<String> stackTrace) {
    this.errorMessage = errorMessage;
    this.stackTrace = stackTrace;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public List<String> getStackTrace() {
    return stackTrace;
  }
}
