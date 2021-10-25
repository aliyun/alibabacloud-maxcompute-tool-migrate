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

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.Function;
import com.aliyun.odps.Resource;

public class McFunctionInfo {
  private String functionName;
  private String className;
  private List<String> useList;

  public McFunctionInfo(String functionName, String className, List<String> useList) {
    this.functionName = functionName;
    this.className = className;
    this.useList = useList;
  }

  public McFunctionInfo(Function function) {
    // todo check getResourceNames
    List<String> resources = new ArrayList<>();
    function.getResourceNames();
    for (Resource resource : function.getResources()) {
      resources.add(resource.getName());
    }
    this.functionName = function.getName();
    this.className = function.getClassPath();
    this.useList = resources;
  }

  public Function toFunction() {
    Function function = new Function();
    function.setName(functionName);
    function.setClassPath(className);
    function.setResources(useList);
    return function;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getClassName() {
    return className;
  }

  public List<String> getUseList() {
    return useList;
  }

}
