/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//package com.aliyun.odps.mma.server.action;

//import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_FUNCTION_FOLDER;
//import static com.aliyun.odps.datacarrier.taskscheduler.Constants.EXPORT_META_FILE_NAME;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.apache.commons.lang.exception.ExceptionUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import com.aliyun.odps.Function;
//import com.aliyun.odps.Resource;
//import com.aliyun.odps.datacarrier.taskscheduler.GsonUtils;
//import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
//import com.aliyun.odps.datacarrier.taskscheduler.OssUtils;

//
//public class OdpsExportFunctionAction extends DefaultAction {
//  private static final Logger LOG = LogManager.getLogger(OdpsExportFunctionAction.class);
//
//  private String taskName;
//  private Function function;
//
//  public OdpsExportFunctionAction(String id, String taskName, Function func) {
//    super(id);
//    this.taskName = taskName;
//    this.function = func;
//  }
//
//  @Override
//  public Object call() throws MmaException {
//    try {
//      List<String> resources = new ArrayList<>();
//      for (Resource resource : function.getResources()) {
//        resources.add(resource.getName());
//      }
//
//      OdpsFunctionInfo functionInfo =
//          new OdpsFunctionInfo(function.getName(), function.getClassPath(), resources);
//      String ossFileName = OssUtils.getOssPathToExportObject(
//          taskName,
//          EXPORT_FUNCTION_FOLDER,
//          function.getProject(),
//          function.getName(),
//          EXPORT_META_FILE_NAME);
//      String content = GsonUtils.toJson(functionInfo);
//      LOG.info("Task: {}, function info: {}", id, content);
//      OssUtils.createFile(actionExecutionContext.getOssConfig(), ossFileName, content);
//      setProgress(ActionProgress.SUCCEEDED);
//    } catch (Exception e) {
//      LOG.error("Action failed, actionId: {}, stack trace: {}",
//                id, ExceptionUtils.getFullStackTrace(e));
//      setProgress(ActionProgress.FAILED);
//    }
//
//    return null;
//  }
//
//  @Override
//  public String getName() {
//    return "Function exporting";
//  }
//}
