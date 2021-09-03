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