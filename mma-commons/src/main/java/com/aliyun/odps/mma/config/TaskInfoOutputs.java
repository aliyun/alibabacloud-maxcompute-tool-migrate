package com.aliyun.odps.mma.config;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class TaskInfoOutputs {
	  @Expose
	  @SerializedName("taskId")
	  String taskId;

	  @Expose
	  @SerializedName("startTime")
	  Long startTime;

	  @Expose
	  @SerializedName("endTime")
	  Long endTime;

	  @Expose
	  @SerializedName("Progress")
	  String progress;
	  

	  public TaskInfoOutputs(
	      String taskId,
	      Long startTime,
	      Long endTime,
	      String progress) {
	    this.taskId = taskId;
	    this.startTime = startTime;
	    this.endTime = endTime;
	    this.progress = progress;
	  }

	  public String gettaskId() {
	    return this.taskId;
	  }

	  public Long getstartTime() {
	    return startTime;
	  }

	  public Long getendTime() {
	    return endTime;
	  }

	  public String getProgress() {
	    return this.progress;
	  }
}
