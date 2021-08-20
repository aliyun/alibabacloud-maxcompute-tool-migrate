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

package com.aliyun.odps.mma.server.ui.tasks;

import static j2html.TagCreator.a;
import static j2html.TagCreator.div;
import static j2html.TagCreator.h4;
import static j2html.TagCreator.li;
import static j2html.TagCreator.span;
import static j2html.TagCreator.strong;
import static j2html.TagCreator.td;
import static j2html.TagCreator.th;
import static j2html.TagCreator.tr;
import static j2html.TagCreator.ul;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import com.aliyun.odps.mma.server.JobScheduler;
import com.aliyun.odps.mma.server.action.Action;
import com.aliyun.odps.mma.server.action.ActionProgress;
import com.aliyun.odps.mma.server.action.info.AbstractActionInfo;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.ui.ActionGraph;
import com.aliyun.odps.mma.server.ui.PagedDataSource;
import com.aliyun.odps.mma.server.ui.PagedTable;
import com.aliyun.odps.mma.server.ui.WebUiPage;
import com.aliyun.odps.mma.server.ui.WebUiTab;
import com.aliyun.odps.mma.server.ui.utils.UiUtils;
import com.aliyun.odps.mma.util.Constants;
import com.aliyun.odps.utils.StringUtils;
import j2html.attributes.Attribute;
import j2html.tags.DomContent;

public class TaskPage extends WebUiPage {

  private static final Logger LOG = LogManager.getLogger(TaskPage.class);

  private static class ActionTableRowData {
    String id;
    String name;
    Long startTime;
    Long endTime;
    ActionProgress actionProgress;
    AbstractActionInfo actionInfo;

    public ActionTableRowData(
        String id,
        String name,
        Long startTime,
        Long endTime,
        ActionProgress actionProgress,
        AbstractActionInfo actionInfo) {
      this.id = id;
      this.name = name;
      this.startTime = startTime;
      this.endTime = endTime;
      this.actionProgress = actionProgress;
      this.actionInfo = actionInfo;
    }
  }

  private static class ActionDataSource extends PagedDataSource<ActionTableRowData> {
    List<ActionTableRowData> data;

    public ActionDataSource(int pageSize, List<Action> actions) {
      super(pageSize);
      this.data = actions.stream().map(
          a -> new ActionTableRowData(
              a.getId(),
              a.getName(),
              a.getStartTime(),
              a.getEndTime(),
              a.getProgress(),
              a.getActionInfo())
      ).collect(Collectors.toList());
    }

    @Override
    public int dataSize() {
      return data.size();
    }

    @Override
    public List<ActionTableRowData> sliceData(int from, int to) {
      return data.subList(from, to);
    }
  }

  private static class ActionPagedTable extends PagedTable<ActionTableRowData> {

    private String taskId;
    private String tableHeaderId;
    private String parameterPath;
    private int pageSize;

    public ActionPagedTable(
        String taskId,
        String actionTag,
        String tableHeaderId,
        int pageSize,
        String parameterPath,
        List<Action> actions) {
      super(actionTag + "-table",
            actionTag + ".pageSize",
            actionTag + ".prevPageSize",
            actionTag + ".page",
            new ActionDataSource(pageSize, actions));
      this.taskId = taskId;
      this.tableHeaderId = tableHeaderId;
      this.parameterPath = parameterPath;
      this.pageSize = pageSize;
    }

    @Override
    public DomContent[] getHeaders() {
      DomContent[] headers = new DomContent[5];
      headers[0] = th("Action ID");
      headers[1] = th("Action name");
      headers[2] = th("Start time");
      headers[3] = th("Duration");
      headers[4] = th("Status");

      return headers;
    }

    @Override
    public DomContent getRow(ActionTableRowData rowData) {
      return tr(
          td(a(rowData.id).withHref(actionPageLink(rowData.id)).withClass("name-link")),
          td(rowData.name),
          td(UiUtils.formatDate(rowData.startTime)),
          td(UiUtils.formatDuration(rowData.startTime, rowData.endTime)),
          td(rowData.actionProgress.name())
      );
    }

    @Override
    public String pageLink(int page) {
      return parameterPath + "?" +
          Constants.TASK_ID_PARAM + "=" + taskId + "&" +
          getPageNumberFormField() + "=" + page + "&" +
          getPageSizeFormField() + "=" + pageSize +
          "#" + tableHeaderId;
    }

    @Override
    public String goButtonFormPath() {
      return parameterPath;
    }

    @Override
    public Map<String, String> getAdditionalPageNavigationInputs() {
      return StringUtils.isBlank(taskId) ?
          Collections.emptyMap() : Collections.singletonMap(Constants.TASK_ID_PARAM, taskId);
    }

    private String actionPageLink(String id) {
      LOG.debug("parameter path: {}", parameterPath);
      return String.format("%s/action?taskId=%s&actionId=%s", parameterPath, taskId, id);
    }
  }

  private WebUiTab parent;
  private JobScheduler jobScheduler;

  public TaskPage(String prefix, TasksTab parent, JobScheduler jobScheduler) {
    super(prefix);
    this.parent = parent;
    this.jobScheduler = Objects.requireNonNull(jobScheduler);
  }

  private DomContent actionsTable(
      HttpServletRequest request,
      String taskId,
      String tableHeaderId,
      String actionTag,
      List<Action> actions) {

    Map<String, String[]> allParameters = request.getParameterMap();
    LOG.debug("URI: {}", request.getRequestURI());
    LOG.debug("Parameters: ", allParameters.toString());

    String parameterActionPage = request.getParameter(actionTag + ".page");
    String parameterActionPageSize = request.getParameter(actionTag + ".pageSize");
    String parameterActionPrevPageSize = request.getParameter(actionTag + ".prevPageSize");

    int actionPage = parameterActionPage == null ? 1 : Integer.parseInt(parameterActionPage);
    int actionPageSize =
        parameterActionPageSize == null ? 10 : Integer.parseInt(parameterActionPageSize);
    int actionPrevPageSize =
        parameterActionPrevPageSize == null ? actionPageSize : Integer.parseInt(parameterActionPrevPageSize);

    int page = actionPageSize <= actionPrevPageSize ? actionPage : 1;

    return new ActionPagedTable(
        taskId,
        actionTag,
        tableHeaderId,
        actionPageSize,
        String.join("/", parent.getBasePath(), getPrefix()),
        actions
    ).table(page);
  }

  @Override
  public String render(HttpServletRequest request) {
    String parameterTaskId = request.getParameter(Constants.TASK_ID_PARAM);
    String parameterTaskTag = request.getParameter(Constants.TASK_TAG_PARAM);

    if (parameterTaskId == null) {
      throw new IllegalArgumentException("Missing required parameter: " + Constants.TASK_ID_PARAM);
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

    String title = "Details for " + parameterTaskId;
    List<DomContent> content = new LinkedList<>();

    // Summary
    content.add(
        div(
            ul(
                li(
                    strong("Status: "),
                    span(task.getProgress().name())
                ),
                li(
                    strong("Start time: "),
                    span(UiUtils.formatDate(task.getStartTime()))
                ),
                li(
                    strong("Duration: "),
                    span(UiUtils.formatDuration(task.getStartTime(), task.getEndTime()))
                )
                // TODO: show progress, data size, included partitions, etc.
            ).withClass("unstyled")
        )
    );

    // Dag visualization
    content.add(getDagVis(task));

    // Action table
    content.add(
        h4("Actions").withId("actions")
    );
    List<Action> actions = new LinkedList<>();
    Iterator<Action> actionIterator = new BreadthFirstIterator<>(task.getDag());
    while (actionIterator.hasNext()) {
      actions.add(actionIterator.next());
    }
    content.add(
        actionsTable(
            request,
            task.getId(),
            "running",
            "runningAction",
            actions
        )
    );

    return UiUtils.basicMmaPage(title, content, parent);
  }

  private DomContent getDagVis(Task task) {
    return div(
        span(
            span().withClass("expand-dag-viz-arrow arrow-closed"),
            a("DAG Visualization")
                .attr(new Attribute("data-toggle", "tooltip"))
                .attr(new Attribute("data-placement", "right"))
        ).withId("task-dag-viz")
         .withClass("expand-dag-viz")
         .attr(new Attribute("onclick", "toggleDagViz()")),
        div().withId("dag-viz-graph"),
        div(
            div(
                div(getDotFile(task)).withClass("dot-file")
                // TODO: handle action progress
                // TODO: task id
            ).withClass("task-metadata").attr(new Attribute("task-id", task.getId()))
        ).withId("dag-viz-metadata").withStyle("display:none")
    );
  }

  private String getDotFile(Task task) {
    DirectedAcyclicGraph<Action, DefaultEdge> dag = task.getDag();
    return new ActionGraph(dag).toDotFile();
  }
}
