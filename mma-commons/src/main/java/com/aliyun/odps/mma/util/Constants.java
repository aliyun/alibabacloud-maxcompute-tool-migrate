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

package com.aliyun.odps.mma.util;

public class Constants {

  /**
   * UI & RESTFul API parameters
   */
  public static final String ROOT_JOB_ID_PARAM = "rootJobId";
  public static final String JOB_ID_PARAM = "jobId";
  public static final String JOB_TAG_PARAM = "jobTag";
  public static final String TASK_ID_PARAM = "taskId";
  public static final String TASK_TAG_PARAM = "taskTag";
  public static final String PERMANENT_PARAM = "permanent";
  public static final String FORCE_PARAM = "force";
  public static final String ACTION_PARAM = "action";

  /**
   * Available actions of HTTP PUT method
   */
  public static final String RESET_ACTION = "reset";
  public static final String UPDATE_ACTION = "update";
}
