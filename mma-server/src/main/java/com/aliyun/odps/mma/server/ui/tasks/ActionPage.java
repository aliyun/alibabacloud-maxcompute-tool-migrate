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

package com.aliyun.odps.mma.server.ui.tasks;

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

import com.aliyun.odps.mma.server.JobScheduler;
import com.aliyun.odps.mma.server.action.Action;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.ui.WebUiPage;
import com.aliyun.odps.mma.server.ui.WebUiTab;
import com.aliyun.odps.mma.server.ui.utils.UiUtils;
import j2html.TagCreator;
import j2html.tags.DomContent;

public class ActionPage extends WebUiPage {

  private WebUiTab parent;
  private JobScheduler jobScheduler;

  public ActionPage(String prefix, WebUiTab parent, JobScheduler jobScheduler) {
    super(prefix);
    this.parent = parent;
    this.jobScheduler = jobScheduler;
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
        tasks = jobScheduler.getFailedTasks();
        break;
      case "succeededTask":
        tasks = jobScheduler.getSucceededTasks();
        break;
      case "canceledTask":
        tasks = jobScheduler.getCanceledTasks();
        break;
      case "runningTask":
      default:
        // The status of a running task may changed to failed, succeeded, or canceled when the page
        // is refreshed. To avoid a 'Parameter taskTag not found' error, need to search all the
        // tasks.
        tasks = Stream.of(jobScheduler.getRunningTasks(),
                          jobScheduler.getFailedTasks(),
                          jobScheduler.getSucceededTasks(),
                          jobScheduler.getCanceledTasks())
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
            TagCreator.span(UiUtils.formatDuration(action.getStartTime(), action.getEndTime()))
        )
    );
    DomContent info = UiUtils.actionInfoTable(action);
    if (info != null) {
      listEntries.add(
          li(
              strong("Info: "),
              info
          )
      );
    }

    String title = "Details for " + action.getId();
    content.add(
        div(
            ul(
                listEntries.toArray(new DomContent[0])
            ).withClass("unstyled")
        )
    );

    return UiUtils.basicMmaPage(title, content, parent);
  }
}
