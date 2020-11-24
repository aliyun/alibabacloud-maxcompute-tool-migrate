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

package com.aliyun.odps.datacarrier.taskscheduler.ui.tasks;

import static j2html.TagCreator.div;
import static j2html.TagCreator.li;
import static j2html.TagCreator.span;
import static j2html.TagCreator.strong;
import static j2html.TagCreator.ul;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;

import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.task.Task;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUIPage;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUITab;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.UIUtils;
import j2html.tags.DomContent;

public class ActionPage extends WebUIPage {

  private WebUITab parent;
  private TaskScheduler taskScheduler;

  public ActionPage(String prefix, WebUITab parent, TaskScheduler taskScheduler) {
    super(prefix);
    this.parent = parent;
    this.taskScheduler = taskScheduler;
  }

  @Override
  public String render(HttpServletRequest request) {
    String parameterTaskId = request.getParameter("taskId");
    String parameterActionId = request.getParameter("actionId");
    String parameterTaskTag = request.getParameter("taskTag");

    if (parameterTaskId == null) {
      throw new IllegalArgumentException("Parameter taskId not found");
    }

    if (parameterActionId == null) {
      throw new IllegalArgumentException("Parameter actionId not found");
    }

    if (parameterTaskTag == null) {
      // Default
      parameterTaskTag = "";
    }

    // Find the task
    List<Task> tasks;
    switch (parameterTaskTag) {
      case "failedTask":
        tasks = taskScheduler.getFailedTasks();
        break;
      case "succeededTask":
        tasks = taskScheduler.getSucceededTasks();
        break;
      case "canceledTask":
        tasks = taskScheduler.getCanceledTasks();
        break;
      case "runningTask":
      default:
        // The status of a running task may changed to failed, succeeded, or canceled when the page
        // is refreshed. To avoid a 'Parameter taskTag not found' error, need to search all the
        // tasks.
        tasks = Stream.of(taskScheduler.getRunningTasks(),
                          taskScheduler.getFailedTasks(),
                          taskScheduler.getSucceededTasks(),
                          taskScheduler.getCanceledTasks())
                      .flatMap(List::stream)
                      .collect(Collectors.toList());
    }

    Task task = null;
    for (Task t : tasks) {
      if (parameterTaskId.equals(t.getId())) {
        task = t;
      }
    }
    if (task == null) {
      throw new IllegalArgumentException("Task " + parameterTaskId + " not found");
    }

    // Find the action
    Action action = null;
    DirectedAcyclicGraph<Action, DefaultEdge> dag = task.getDag();
    for (Action a : dag) {
      if (parameterActionId.equals(a.getId())) {
        action = a;
      }
    }
    if (action == null) {
      throw new IllegalArgumentException("Action " + parameterActionId + " not found");
    }

    List<DomContent> content = new LinkedList<>();
    List<DomContent> listEntries = new LinkedList<>();
    listEntries.add(
        li(
            strong("Status: "),
            span(action.getProgress().name())
        )
    );
    listEntries.add(
        li(
            strong("Duration: "),
            span(UIUtils.formatDuration(action.getStartTime(), action.getEndTime()))
        )
    );
    DomContent info = UIUtils.actionInfoTable(action);
    if (info != null) {
      listEntries.add(
          li(
              strong("Info: "),
              info
          )
      );
    }

    String title = "Details for " + action.getName();
    content.add(
        div(
            ul(
                listEntries.toArray(new DomContent[0])
            ).withClass("unstyled")
        )
    );

    return UIUtils.basicMmaPage(title, content, parent);
  }
}
