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
import static j2html.TagCreator.h4;
import static j2html.TagCreator.td;
import static j2html.TagCreator.th;
import static j2html.TagCreator.tr;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.datacarrier.taskscheduler.TaskScheduler;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.action.HiveSourceVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.HiveUdtfDataTransferAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsCreateTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDestVerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.OdpsDropTableAction;
import com.aliyun.odps.datacarrier.taskscheduler.action.VerificationAction;
import com.aliyun.odps.datacarrier.taskscheduler.task.Task;
import com.aliyun.odps.datacarrier.taskscheduler.task.TaskProgress;
import com.aliyun.odps.datacarrier.taskscheduler.ui.PagedDataSource;
import com.aliyun.odps.datacarrier.taskscheduler.ui.PagedTable;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUIPage;
import com.aliyun.odps.datacarrier.taskscheduler.ui.WebUITab;
import com.aliyun.odps.datacarrier.taskscheduler.ui.utils.UIUtils;
import j2html.tags.DomContent;

public class AllTasksPage extends WebUIPage {
  private static final Logger LOG = LogManager.getLogger(AllTasksPage.class);

  private static class TaskTableRowData {
    String id;
    Long startTime;
    Long endTime;

    public TaskTableRowData(String id, Long startTime, Long endTime) {
      this.id = id;
      this.startTime = startTime;
      this.endTime = endTime;
    }
  }

  private static class TaskDataSource extends PagedDataSource<TaskTableRowData> {
    List<TaskTableRowData> data;

    public TaskDataSource(int pageSize, List<Task> tasks) {
      super(pageSize);
      this.data = tasks
          .stream()
          .map(t -> new TaskTableRowData(t.getId(), t.getStartTime(), t.getEndTime()))
          .sorted((o1, o2) -> {
            if (o1.startTime < o2.startTime) {
              return -1;
            } else if (o1.startTime.equals(o2.startTime)) {
              return 0;
            }
            return 1;
          })
          .collect(Collectors.toList());
    }

    @Override
    public int dataSize() {
      return data.size();
    }

    @Override
    public List<TaskTableRowData> sliceData(int from, int to) {
      return data.subList(from, to);
    }
  }

  private static class TaskPagedTable extends PagedTable<TaskTableRowData> {

    private static final String TABLE_CSS_CLASS = "table table-bordered table-condensed "
        + "table-striped table-head-clickable table-cell-width-limited";

    private String taskTag;
    private String tableHeaderId;
    private String parameterPath;
    private int pageSize;

    public TaskPagedTable(String taskTag,
                          String tableHeaderId,
                          int pageSize,
                          String parameterPath,
                          List<Task> tasks) {
      super(taskTag + "-table",
            TABLE_CSS_CLASS,
            taskTag + ".pageSize",
            taskTag + ".prevPageSize",
            taskTag + ".page",
            new TaskDataSource(pageSize, tasks));
      this.taskTag = taskTag;
      this.tableHeaderId = tableHeaderId;
      this.parameterPath = parameterPath;
      this.pageSize = pageSize;
    }

    @Override
    public DomContent[] getHeaders() {
      DomContent[] headers = new DomContent[3];
      headers[0] = th("Task ID");
      headers[1] = th("Submitted");
      headers[2] = th("Duration");
      return headers;
    }

    @Override
    public DomContent getRow(TaskTableRowData rowData) {
      return tr(
          td(
              a(rowData.id).withHref(taskPageLink(rowData.id)).withClass("name-link")
          ),
          td(UIUtils.formatDate(rowData.startTime)),
          td(UIUtils.formatDuration(rowData.startTime, rowData.endTime))
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

    private String taskPageLink(String id) {
      return String.format("%s/task?taskId=%s&taskTag=%s", parameterPath, id, taskTag);
    }
  }

  private WebUITab parent;
  private TaskScheduler taskScheduler;

  public AllTasksPage(String prefix, WebUITab parent, TaskScheduler taskScheduler) {
    super(prefix);
    this.parent = Objects.requireNonNull(parent);
    this.taskScheduler = Objects.requireNonNull(taskScheduler);
  }

  public DomContent tasksTable(
      HttpServletRequest request,
      String tableHeaderId,
      String taskTag,
      List<Task> tasks) {
    Map<String, String[]> allParameters = request.getParameterMap();
    LOG.info("URI: {}", request.getRequestURI());
    LOG.info("Parameters: ", allParameters.toString());

    String parameterTaskPage = request.getParameter(taskTag + ".page");
    String parameterTaskPageSize = request.getParameter(taskTag + ".pageSize");
    String parameterTaskPrevPageSize = request.getParameter(taskTag + ".prevPageSize");

    int taskPage = parameterTaskPage == null ? 1 : Integer.parseInt(parameterTaskPage);
    int taskPageSize =
        parameterTaskPageSize == null ? 10 : Integer.parseInt(parameterTaskPageSize);
    int taskPrevPageSize =
        parameterTaskPrevPageSize == null ? taskPageSize : Integer.parseInt(parameterTaskPrevPageSize);

    // Set page to 1 if page size is modified
    int page = taskPageSize <= taskPrevPageSize ? taskPage : 1;

    return new TaskPagedTable(
      taskTag,
      tableHeaderId,
      taskPageSize,
      String.join("/", parent.getBasePath(), parent.getPrefix()),
      tasks
    ).table(page);
  }

  @Override
  public String render(HttpServletRequest request) {

    List<DomContent> content = new LinkedList<>();

    List<Task> runningTasks = taskScheduler.getRunningTasks();
    content.add(
        h4("Running Tasks (" + runningTasks.size() + ")").withId("running")
    );
    content.add(
        tasksTable(
            request,
            "running",
            "runningTask",
            runningTasks
        )
    );
    List<Task> failedTasks = taskScheduler.getFailedTasks();
    content.add(
        h4("Failed Tasks (" + failedTasks.size() + ")").withId("failed")
    );
    content.add(
        tasksTable(
            request,
            "failed",
            "failedTask",
            failedTasks
        )
    );
    List<Task> succeededTasks = taskScheduler.getSucceededTasks();
    content.add(
        h4("Succeeded Tasks (" + succeededTasks.size() + ")").withId("succeeded")
    );
    content.add(
        tasksTable(
            request,
            "succeeded",
            "succeededTask",
            succeededTasks
        )
    );
    List<Task> canceledTasks = taskScheduler.getCanceledTasks();
    content.add(
        h4("Canceled Tasks (" + canceledTasks.size() + ")").withId("canceled")
    );
    content.add(
        tasksTable(
            request,
            "canceled",
            "canceledTask",
            canceledTasks
        )
    );
    return UIUtils.basicMmaPage("Tasks", content, parent);
  }
}
