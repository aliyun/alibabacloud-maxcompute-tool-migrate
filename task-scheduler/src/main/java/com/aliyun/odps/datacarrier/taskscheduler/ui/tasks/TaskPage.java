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

import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.ActionProgress;
import com.aliyun.odps.datacarrier.taskscheduler.action.info.AbstractActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.task.Task;
import com.aliyun.odps.datacarrier.taskscheduler.ui.ActionGraph;
import com.aliyun.odps.datacarrier.taskscheduler.ui.PagedDataSource;
import com.aliyun.odps.datacarrier.taskscheduler.ui.PagedTable;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUIPage;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUITab;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.UIUtils;
import j2html.attributes.Attribute;
import j2html.tags.DomContent;

public class TaskPage extends WebUIPage {
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

    private static final String TABLE_CSS_CLASS = "table table-bordered table-condensed "
        + "table-striped table-head-clickable table-cell-width-limited";

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
            TABLE_CSS_CLASS,
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
      DomContent[] headers = new DomContent[4];
      headers[0] = th("Action name");
      headers[1] = th("Start time");
      headers[2] = th("Duration");
      headers[3] = th("Status");

      return headers;
    }

    @Override
    public DomContent getRow(ActionTableRowData rowData) {
      return tr(
          td(a(rowData.name).withHref(actionPageLink(rowData.id)).withClass("name-link")),
          td(UIUtils.formatDate(rowData.startTime)),
          td(UIUtils.formatDuration(rowData.startTime, rowData.endTime)),
          td(rowData.actionProgress.name())
      );
    }

    @Override
    public String pageLink(int page) {
      return parameterPath + "/?" +
          getPageNumberFormField() + "=" + page + "&" +
          getPageSizeFormField() + "=" + pageSize +
          "#" + tableHeaderId;
    }

    @Override
    public String goButtonFormPath() {
      return parameterPath;
    }

    private String actionPageLink(String id) {
      LOG.info("parameter path: {}", parameterPath);
      return String.format("%s/action?taskId=%s&actionId=%s", parameterPath, taskId, id);
    }
  }

  private WebUITab parent;
  private TaskScheduler taskScheduler;

  public TaskPage(String prefix, TasksTab parent, TaskScheduler taskScheduler) {
    super(prefix);
    this.parent = parent;
    this.taskScheduler = Objects.requireNonNull(taskScheduler);
  }

  private DomContent actionsTable(
      HttpServletRequest request,
      String taskId,
      String tableHeaderId,
      String actionTag,
      List<Action> actions) {
    Map<String, String[]> allParameters = request.getParameterMap();
    LOG.info("URI: {}", request.getRequestURI());
    LOG.info("Parameters: ", allParameters.toString());

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
    String parameterTaskId = request.getParameter("taskId");
    String parameterTaskTag = request.getParameter("taskTag");

    if (parameterTaskId == null) {
      throw new IllegalArgumentException("Parameter taskId not found");
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
                    span(UIUtils.formatDate(task.getStartTime()))
                ),
                li(
                    strong("Duration: "),
                    span(UIUtils.formatDuration(task.getStartTime(), task.getEndTime()))
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

    return UIUtils.basicMmaPage(title, content, parent);
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
