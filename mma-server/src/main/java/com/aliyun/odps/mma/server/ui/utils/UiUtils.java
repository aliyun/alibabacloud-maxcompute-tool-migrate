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

package com.aliyun.odps.mma.server.ui.utils;

import static j2html.TagCreator.a;
import static j2html.TagCreator.body;
import static j2html.TagCreator.div;
import static j2html.TagCreator.h3;
import static j2html.TagCreator.header;
import static j2html.TagCreator.html;
import static j2html.TagCreator.li;
import static j2html.TagCreator.link;
import static j2html.TagCreator.meta;
import static j2html.TagCreator.p;
import static j2html.TagCreator.script;
import static j2html.TagCreator.span;
import static j2html.TagCreator.strong;
import static j2html.TagCreator.td;
import static j2html.TagCreator.th;
import static j2html.TagCreator.title;
import static j2html.TagCreator.tr;
import static j2html.TagCreator.ul;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.server.action.Action;
import com.aliyun.odps.mma.server.action.info.AbstractActionInfo;
import com.aliyun.odps.mma.server.action.info.CopyTaskActionInfo;
import com.aliyun.odps.mma.server.action.info.HiveSqlActionInfo;
import com.aliyun.odps.mma.server.action.info.McSqlActionInfo;
import com.aliyun.odps.mma.server.action.info.VerificationActionInfo;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.job.Job;
import com.aliyun.odps.mma.server.job.PartitionJob;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;
import com.aliyun.odps.mma.server.ui.PagedDataSource;
import com.aliyun.odps.mma.server.ui.PagedTable;
import com.aliyun.odps.mma.server.ui.WebUiTab;
import com.aliyun.odps.mma.util.Constants;
import com.aliyun.odps.mma.util.GsonUtils;
import j2html.tags.ContainerTag;
import j2html.tags.DomContent;

public class UiUtils {
  private static final DateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

  /**
   * ------------------------ General Utils -------------------------------------------------------
   */
  public String decodeURLParameter(String urlParam) throws UnsupportedEncodingException {
    return URLDecoder.decode(urlParam, "UTF-8");
  }

  public static String formatDate(Long timestamp) {
    if (timestamp == null || timestamp <= 0L) {
      return "N/A";
    }
    return DEFAULT_DATE_FORMAT.format(new Date(timestamp));
  }

  public static String formatDuration(Long startTime, Long endTime) {
    if (startTime == null || startTime <= 0L) {
      return "N/A";
    }

    Long ms;
    if (endTime == null || endTime <= 0L || endTime < startTime) {
      ms = System.currentTimeMillis() - startTime;
    } else {
      ms = endTime - startTime;
    }

    if (ms < 100){
      return String.format("%d ms", ms);
    }
    Double seconds = ms.doubleValue() / 1000;
    if (seconds < 1) {
      return String.format("%.1f s", seconds);
    }
    if (seconds < 60) {
      return String.format("%.0f s", seconds);
    }
    Double minutes = seconds / 60;
    if (minutes < 10) {
      return String.format("%.1f min", minutes);
    }
    if (minutes < 60) {
      return String.format("%.0f min", minutes);
    }
    Double hours = minutes / 60;
    return String.format("%.1f h", hours);
  }

  /**
   * ------------------------ Action Utils --------------------------------------------------------
   */
  private static DomContent actionInfoEntry(String key, String val) {
    if (val == null) {
      val = "N/A";
    }

    return li(
        strong(key + ": "),
        span(val).withStyle("word-break: break-all;")
    );
  }

  private static DomContent actionInfoEntry(String key, DomContent val) {
    if (val == null) {
      val = span("N/A");
    }

    return li(
        strong(key + ": "),
        val
    );
  }

  public static DomContent actionInfoTable(Action action) {
    AbstractActionInfo actionInfo = action.getActionInfo();

    if (actionInfo instanceof HiveSqlActionInfo) {
      HiveSqlActionInfo hiveSqlActionInfo = (HiveSqlActionInfo) actionInfo;

      List<DomContent> listEntries = new LinkedList<>();

      String jobId = hiveSqlActionInfo.getJobId();
      if (!StringUtils.isBlank(jobId)) {
        listEntries.add(
            actionInfoEntry("Hive job ID", hiveSqlActionInfo.getJobId())
        );
      } else {
        listEntries.add(
            actionInfoEntry("Hive job name", action.getId())
        );
      }

      String trackingUrl = hiveSqlActionInfo.getTrackingUrl();
      if (!StringUtils.isBlank(trackingUrl)) {
        listEntries.add(
            actionInfoEntry("Hive job tracking URL",
                            a(hiveSqlActionInfo.getTrackingUrl()).withStyle("word-break: break-all;"))
        );
      }
      // TODO: progress

      return ul(
        listEntries.toArray(new DomContent[0])
      );
    } else if (actionInfo instanceof McSqlActionInfo) {
      McSqlActionInfo mcSqlActionInfo = (McSqlActionInfo) actionInfo;
      List<DomContent> listEntries = new LinkedList<>();
      listEntries.add(
          actionInfoEntry("MC instance ID", mcSqlActionInfo.getInstanceId())
      );
      if (mcSqlActionInfo.getLogView() != null) {
        listEntries.add(
            actionInfoEntry("MC instance tracking URL",
                            a(mcSqlActionInfo.getLogView()).withStyle("word-break: break-all;"))
        );
      } else {
        listEntries.add(
            actionInfoEntry("MC instance tracking URL", "N/A")
        );
      }
      // TODO: progress

      return ul(
          listEntries.toArray(new DomContent[0])
      );
    } else if (action instanceof CopyTaskActionInfo) {
      CopyTaskActionInfo mcSqlActionInfo = (CopyTaskActionInfo) actionInfo;
      List<DomContent> listEntries = new LinkedList<>();
      listEntries.add(
              actionInfoEntry("MC instance ID", mcSqlActionInfo.getInstanceId())
      );
      if (mcSqlActionInfo.getLogView() != null) {
        listEntries.add(
                actionInfoEntry("MC instance tracking URL",
                        a(mcSqlActionInfo.getLogView()).withStyle("word-break: break-all;"))
        );
      } else {
        listEntries.add(
                actionInfoEntry("MC instance tracking URL", "N/A")
        );
      }
      // TODO: progress

      return ul(
              listEntries.toArray(new DomContent[0])
      );
    } else if (actionInfo instanceof VerificationActionInfo) {
      VerificationActionInfo verificationActionInfo = (VerificationActionInfo) actionInfo;

      if (verificationActionInfo.isPartitioned() == null) {
        return null;
      } else if (verificationActionInfo.isPartitioned()) {
        List<DomContent> listEntries = new LinkedList<>();
        if (verificationActionInfo.passed() != null) {
          listEntries.add(
              actionInfoEntry("Passed", verificationActionInfo.passed().toString())
          );
          listEntries.add(
              actionInfoEntry(
                  "Number of succeeded partitions",
                  Integer.toString(verificationActionInfo.getSucceededPartitions().size()))
          );
          listEntries.add(
              actionInfoEntry(
                  "Number of failed partitions",
                  Integer.toString(verificationActionInfo.getFailedPartitions().size()))

          );
          Map<String, Long> partitionValuesToSourceNumRecord =
              verificationActionInfo.getPartitionValuesToSourceNumRecord();
          Map<String, Long> partitionValuesToDestNumRecord =
              verificationActionInfo.getPartitionValuesToDestNumRecord();
          listEntries.add(
              li(
                  strong("Successful partitions: "),
                  ul(
                      verificationActionInfo
                          .getSucceededPartitions()
                          .stream()
                          .map(pvs -> getPartitionActionInfoEntry(
                              pvs,
                              partitionValuesToSourceNumRecord.get(pvs),
                              partitionValuesToDestNumRecord.get(pvs))
                          )
                          .toArray(DomContent[]::new)
                  )
              )
          );
          listEntries.add(
              li(
                  strong("Failed partitions: "),
                  ul(
                      verificationActionInfo
                          .getFailedPartitions()
                          .stream()
                          .map(pvs -> getPartitionActionInfoEntry(
                              pvs,
                              partitionValuesToSourceNumRecord.get(pvs),
                              partitionValuesToDestNumRecord.get(pvs))
                          )
                          .toArray(DomContent[]::new)
                  )
              )
          );

        } else {
          listEntries.add(
              actionInfoEntry("Passed", "N/A")
          );
        }

        return ul(
            listEntries.toArray(new DomContent[0])
        );
      } else {
        List<DomContent> listEntries = new LinkedList<>();
        if (verificationActionInfo.passed() != null) {
          listEntries.add(
              actionInfoEntry("Passed", verificationActionInfo.passed().toString())
          );
          listEntries.add(
              actionInfoEntry(
                  "Source record number",
                  Long.toString(verificationActionInfo.getSourceNumRecord()))
          );
          listEntries.add(
              actionInfoEntry(
                  "Dest record number",
                  Long.toString(verificationActionInfo.getDestNumRecord()))
          );
        } else {
          listEntries.add(
              actionInfoEntry("Passed", "N/A")
          );
        }
        return ul(
            listEntries.toArray(new DomContent[0])
        );
      }
    } else {
      return null;
    }
  }

  private static DomContent getPartitionActionInfoEntry(
      String partitionValuesStr,
      Long sourceNumRecord,
      Long destNumRecord) {
    String source = sourceNumRecord == null ? "N/A" : sourceNumRecord.toString();
    String dest = destNumRecord == null ? "N/A" : destNumRecord.toString();
    return actionInfoEntry(
        partitionValuesStr,
        String.format("source record number: %s, dest record number: %s", source, dest));
  }
  /**
   * ------------------------ Action Utils Ends ---------------------------------------------------
   */

  /**
   * ------------------------ Task Utils ----------------------------------------------------------
   */
  public static class TaskTableRowData {
    String id;
    String status;
    Long startTime;
    Long endTime;

    TaskTableRowData(String id, String status, Long startTime, Long endTime) {
      this.id = id;
      this.status = status;
      this.startTime = startTime;
      this.endTime = endTime;
    }
  }

  private static class TaskDataSource extends PagedDataSource<TaskTableRowData> {
    List<TaskTableRowData> data;

    TaskDataSource(int pageSize, List<Task> tasks) {
      super(pageSize);
      this.data = tasks
          .stream()
          .map(t -> new TaskTableRowData(
              t.getId(), t.getProgress().name(), t.getStartTime(), t.getEndTime()))
          .sorted((o1, o2) -> {
            if (o1.startTime == null) {
              return -1;
            } else if (o2.startTime == null) {
              return 1;
            } else if (o1.startTime < o2.startTime) {
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

    private String rootJobId;
    private String jobId;
    private String taskTag;
    private String tableHeaderId;
    private String parameterPath;
    private int pageSize;

    TaskPagedTable(String taskTag,
                   String tableHeaderId,
                   int pageSize,
                   String parameterPath,
                   String rootJobId,
                   String jobId,
                   List<Task> tasks) {
      super(taskTag + "-table",
            taskTag + ".pageSize",
            taskTag + ".prevPageSize",
            taskTag + ".page",
            new TaskDataSource(pageSize, tasks));
      this.taskTag = taskTag;
      this.tableHeaderId = tableHeaderId;
      this.parameterPath = parameterPath;
      this.rootJobId = rootJobId;
      this.jobId = jobId;
      this.pageSize = pageSize;
    }

    @Override
    public DomContent[] getHeaders() {
      DomContent[] headers = new DomContent[4];
      headers[0] = th("Task ID");
      headers[1] = th("Status");
      headers[2] = th("Submitted");
      headers[3] = th("Duration");
      return headers;
    }

    @Override
    public DomContent getRow(TaskTableRowData rowData) {
      ContainerTag taskIdTd;
      // The detailed task page may not be available when the task is still pending
      if (TaskProgress.PENDING.name().equalsIgnoreCase(rowData.status)) {
        taskIdTd = td(rowData.id);
      } else {
        taskIdTd = td(a(rowData.id).withHref(taskPageLink(rowData.id)).withClass("name-link"));
      }
      return tr(
          taskIdTd,
          td(rowData.status),
          td(UiUtils.formatDate(rowData.startTime)),
          td(UiUtils.formatDuration(rowData.startTime, rowData.endTime))
      );
    }

    @Override
    public String pageLink(int page) {
      StringBuilder sb = new StringBuilder(parameterPath);
      sb.append("?")
        .append(getPageNumberFormField()).append("=").append(page)
        .append("&")
        .append(getPageSizeFormField()).append("=").append(pageSize);
      if (!StringUtils.isBlank(jobId)) {
        sb.append("&")
          .append(Constants.ROOT_JOB_ID_PARAM).append("=")
          .append(StringUtils.defaultIfBlank(rootJobId, ""))
          .append("&")
          .append(Constants.JOB_ID_PARAM).append("=").append(jobId);
      }
      sb.append("#")
        .append(tableHeaderId);
      return sb.toString();
    }

    @Override
    public String goButtonFormPath() {
      return parameterPath;
    }

    @Override
    public Map<String, String> getAdditionalPageNavigationInputs() {
      if (StringUtils.isBlank(jobId)) {
        // jobId is not null means this table belongs to a job page
        return Collections.emptyMap();
      }

      Map<String, String> ret = new HashMap<>();
      ret.put(Constants.ROOT_JOB_ID_PARAM, rootJobId);
      ret.put(Constants.JOB_ID_PARAM, jobId);
      return ret;
    }

    private String taskPageLink(String id) {
      return String.format("/tasks/task?taskId=%s&taskTag=%s", id, taskTag);
    }
  }

  public static DomContent tasksTable(
      String parameterPath,
      String rootJobId,
      String jobId,
      HttpServletRequest request,
      String tableHeaderId,
      String taskTag,
      List<Task> tasks,
      Logger logger) {
    Map<String, String[]> allParameters = request.getParameterMap();

    logger.debug("URI: {}", request.getRequestURI());
    logger.debug("Parameters: ", allParameters.toString());

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
        parameterPath,
        rootJobId,
        jobId,
        tasks
    ).table(page);
  }
  /**
   * ------------------------ Task Utils Ends -----------------------------------------------------
   */

  /**
   * ------------------------ Job Utils -----------------------------------------------------------
   */
  public static class JobDataSource extends PagedDataSource<Job> {
    List<Job> data;

    JobDataSource(int pageSize, List<Job> jobs) {
      super(pageSize);
      this.data = jobs
          .stream()
          .sorted((o1, o2) -> {
            Long startTime1 = o1.getStartTime();
            Long startTime2 = o2.getStartTime();
            if (startTime1 == null) {
              return -1;
            } else if (startTime2 == null) {
              return 1;
            } else if (startTime1 > startTime2) {
              return 1;
            } else if (startTime1.equals(startTime2)) {
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
    public List<Job> sliceData(int from, int to) {
      return data.subList(from, to);
    }
  }

  public static class JobPagedTable extends PagedTable<Job> {

    private String rootJobId;
    private String jobId;
    private String jobTag;
    private String tableHeaderId;
    private String parameterPath;
    private int pageSize;

    JobPagedTable(
        String jobTag,
        String tableHeaderId,
        int pageSize,
        String parameterPath,
        String rootJobId,
        String jobId,
        List<Job> jobs) {
      super(jobTag + "-table",
            jobTag + ".pageSize",
            jobTag + ".prevPageSize",
            jobTag + ".page",
            new JobDataSource(pageSize, jobs));
      this.jobTag = jobTag;
      this.tableHeaderId = tableHeaderId;
      this.parameterPath = parameterPath;
      this.pageSize = pageSize;
      this.rootJobId = rootJobId;
      this.jobId = jobId;
    }

    @Override
    public DomContent[] getHeaders() {
      DomContent[] headers = new DomContent[7];
      headers[0] = th("Job ID");
      headers[1] = th("Status");
      headers[2] = th("Object Type");
      headers[3] = th("Source");
      headers[4] = th("Destination");
      headers[5] = th("Start Time");
      headers[6] = th("Duration");
      return headers;
    }

    @Override
    public DomContent getRow(Job job) {
      ContainerTag jobIdTd;
      if (job instanceof PartitionJob) {
        jobIdTd = td(job.getId());
      } else {
        jobIdTd = td(a(job.getId()).withHref(jobPageLink(job.getId())).withClass("name-link"));
      }

      return tr(
          jobIdTd,
          td(job.getStatus().name()),
          td(job.getJobConfiguration().get(JobConfiguration.OBJECT_TYPE)),
          td(UiUtils.getSource(job)),
          td(UiUtils.getDestination(job)),
          td(UiUtils.formatDate(job.getStartTime())),
          td(UiUtils.formatDuration(job.getStartTime(), job.getEndTime()))
      );
    }

    @Override
    public String pageLink(int page) {
      return parameterPath
          // jobId is not null means this table belongs to a job page
          + (StringUtils.isBlank(jobId) ? "" : "/job")
          + "?"
          + (StringUtils.isBlank(rootJobId) ? "" : Constants.ROOT_JOB_ID_PARAM + "=" + rootJobId + "&")
          + (StringUtils.isBlank(jobId) ? "" : Constants.JOB_ID_PARAM + "=" + jobId + "&")
          + getPageNumberFormField() + "=" + page + "&"
          + getPageSizeFormField() + "=" + pageSize
          + "#" + tableHeaderId;
    }

    @Override
    public String goButtonFormPath() {
      return parameterPath
          // jobId is not null means this table belongs to a job page
          + (StringUtils.isBlank(jobId) ? "" : "/job");
    }

    @Override
    public Map<String, String> getAdditionalPageNavigationInputs() {
      if (StringUtils.isBlank(jobId)) {
        // jobId is not null means this table belongs to a job page
        return Collections.emptyMap();
      }

      Map<String, String> ret = new HashMap<>();
      ret.put(Constants.ROOT_JOB_ID_PARAM, rootJobId);
      ret.put(Constants.JOB_ID_PARAM, jobId);
      return ret;
    }

    private String jobPageLink(String id) {
      String root = StringUtils.defaultIfBlank(
          rootJobId,
          StringUtils.defaultIfBlank(jobId, ""));
      return parameterPath + "/job?"
          + Constants.JOB_TAG_PARAM + "=" + jobTag
          + "&" + Constants.ROOT_JOB_ID_PARAM + "=" + root
          + "&" + Constants.JOB_ID_PARAM + "=" + id;
    }
  }

  public static DomContent jobsTable(
      String parameterPath,
      HttpServletRequest request,
      String tableHeaderId,
      String jobTag,
      String rootJobId,
      String jobId,
      List<Job> jobs,
      Logger logger) {

    Map<String, String[]> allParameters = request.getParameterMap();
    logger.debug("URI: {}", request.getRequestURI());
    logger.debug("Parameters: ", allParameters.toString());

    String parameterJobPage = request.getParameter(jobTag + ".page");
    String parameterJobPageSize = request.getParameter(jobTag + ".pageSize");
    String parameterJobPrevPageSize = request.getParameter(jobTag + ".prevPageSize");

    int jobPage = parameterJobPage == null ? 1 : Integer.parseInt(parameterJobPage);
    int jobPageSize =
        parameterJobPageSize == null ? 10 : Integer.parseInt(parameterJobPageSize);
    int jobPrevPageSize =
        parameterJobPrevPageSize == null ? jobPageSize : Integer.parseInt(parameterJobPrevPageSize);

    // Set page to 1 if page size is modified
    int page = jobPageSize <= jobPrevPageSize ? jobPage : 1;
    return new JobPagedTable(
        jobTag,
        tableHeaderId,
        jobPageSize,
        parameterPath,
        rootJobId,
        jobId,
        jobs
    ).table(page);
  }

  public static String getSource(Job job) {
    JobConfiguration config = job.getJobConfiguration();
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG:
      case RESOURCE:
      case FUNCTION:
        return config.get(JobConfiguration.SOURCE_CATALOG_NAME);
      case PARTITION: {
        String catalogName = StringUtils.defaultString(
            config.get(JobConfiguration.SOURCE_CATALOG_NAME),
            "N/A");
        String partitionIdentifier = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
        String tableName = ConfigurationUtils
            .getTableNameFromPartitionIdentifier(partitionIdentifier);
        List<String> partitionValues =
            ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(partitionIdentifier);
        return String.format(
            "%s.%s partition %s",
            catalogName,
            tableName,
            GsonUtils.GSON.toJson(partitionValues));
      }
      default: {
        String catalogName = StringUtils.defaultString(
            config.get(JobConfiguration.SOURCE_CATALOG_NAME),
            "N/A");
        String objectName = StringUtils.defaultString(
            config.get(JobConfiguration.SOURCE_OBJECT_NAME),
            "N/A");
        return String.format("%s.%s", catalogName, objectName);
      }
    }
  }

  public static String getDestination(Job job) {
    JobConfiguration config = job.getJobConfiguration();
    ObjectType objectType = ObjectType.valueOf(config.get(JobConfiguration.OBJECT_TYPE));
    switch (objectType) {
      case CATALOG:
      case RESOURCE:
      case FUNCTION:
        return config.get(JobConfiguration.DEST_CATALOG_NAME);
      case PARTITION: {
        String catalogName = StringUtils.defaultString(
            config.get(JobConfiguration.DEST_CATALOG_NAME),
            "N/A");
        String partitionIdentifier = config.get(JobConfiguration.DEST_OBJECT_NAME);
        String tableName = ConfigurationUtils
            .getTableNameFromPartitionIdentifier(partitionIdentifier);
        List<String> partitionValues =
            ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(partitionIdentifier);
        return String.format(
            "%s.%s partition %s",
            catalogName,
            tableName,
            GsonUtils.GSON.toJson(partitionValues));
      }
      default: {
        String catalogName = StringUtils.defaultString(
            config.get(JobConfiguration.DEST_CATALOG_NAME),
            "N/A");
        String objectName = StringUtils.defaultString(
            config.get(JobConfiguration.DEST_OBJECT_NAME),
            "N/A");
        return String.format("%s.%s", catalogName, objectName);
      }
    }
  }
  /**
   * ------------------------ Job Utils Ends ------------------------------------------------------
   */

  /**
   * ------------------------ Config Utils --------------------------------------------------------
   */
  private static class ConfigDataSource extends PagedDataSource<Entry<String, String>> {

    List<Entry<String, String>> data;

    ConfigDataSource(int pageSize) {
      super(pageSize);
      this.data = new ArrayList<>(MmaServerConfiguration.getInstance().entrySet());
    }

    @Override
    public int dataSize() {
      return data.size();
    }

    @Override
    public List<Entry<String, String>> sliceData(int from, int to) {
      return data.subList(from, to);
    }
  }

  private static class ConfigPagedTable extends PagedTable<Entry<String, String>> {

    private String tableHeaderId;
    private String parameterPath;
    private int pageSize;

    public ConfigPagedTable(
        String tableTag,
        String tableHeaderId,
        int pageSize,
        String parameterPath) {
      super(tableTag + "-table",
            tableTag + ".pageSize",
            tableTag + ".prevPageSize",
            tableTag + ".page",
            new ConfigDataSource(pageSize));
      this.tableHeaderId = tableHeaderId;
      this.parameterPath = parameterPath;
      this.pageSize = pageSize;
    }

    @Override
    public DomContent[] getHeaders() {
      DomContent[] headers = new DomContent[2];
      headers[0] = th("Key");
      headers[1] = th("Value");
      return headers;
    }

    @Override
    public DomContent getRow(Entry<String, String> rowData) {
      // Mask the passwords
      String key = rowData.getKey();
      String value = rowData.getValue();
      if (key.toLowerCase().contains("password") || key.toLowerCase().contains("secret")) {
        value = "******";
      }

      return tr(
          td(rowData.getKey()),
          td(value)
      );
    }

    @Override
    public String pageLink(int page) {
      return parameterPath + "?" +
          getPageNumberFormField() + "=" + page + "&" +
          getPageSizeFormField() + "=" + pageSize +
          "#" + tableHeaderId;
    }

    @Override
    public String goButtonFormPath() {
      return parameterPath;
    }
  }

  public static DomContent configTable(
      String parameterPath,
      HttpServletRequest request,
      String tableHeaderId,
      String tableTag,
      Logger logger) {
    Map<String, String[]> allParameters = request.getParameterMap();

    logger.debug("URI: {}", request.getRequestURI());
    logger.debug("Parameters: ", allParameters.toString());

    String parameterPage = request.getParameter(tableTag + ".page");
    String parameterPageSize = request.getParameter(tableTag + ".pageSize");
    String parameterPrevPageSize = request.getParameter(tableTag + ".prevPageSize");

    int currPage = parameterPage == null ? 1 : Integer.parseInt(parameterPage);
    int pageSize =
        parameterPageSize == null ? 50 : Integer.parseInt(parameterPageSize);
    int prevPageSize =
        parameterPrevPageSize == null ? pageSize : Integer.parseInt(parameterPrevPageSize);

    // Set page to 1 if page size is modified
    int page = pageSize <= prevPageSize ? currPage : 1;

    return new ConfigPagedTable(
        tableTag,
        tableHeaderId,
        pageSize,
        parameterPath
    ).table(page);
  }
  /**
   * ------------------------ Config Utils Ends ---------------------------------------------------
   */

  private static String prependBaseUri(String basePath, String resource) {
    return basePath + "/" + resource + "/";
  }

  public static String basicMmaPage(String title, List<DomContent> content, WebUiTab activeTab) {
    List<DomContent> tags = new LinkedList<>();

    tags.add(
        div(
            div(
                div(
                    p(
                        strong("MC Migration Assistant")
                    ).withClass("navbar-text pull-left")
                ).withClass("brand"),
                ul(
                    activeTab.getHeaderTabs().stream().map(tab ->
                        li(
                            a(tab.getPrefix().toUpperCase())
                                .withHref(prependBaseUri(tab.getBasePath(), tab.getPrefix()))
                        ).withClass(tab == activeTab ? "active" : "")
                    ).toArray(DomContent[]::new)
                ).withClass("nav")
            ).withClass("navbar-inner")
        ).withClass("navbar navbar-static-top")
    );

    content.add(0,
        div(
            div(
                h3(title).withStyle("vertical-align: bottom; display: inline-block;")
            ).withClass("span12")
        ).withClass("row-fluid")
    );
    tags.add(
        div(
            content.toArray(new DomContent[0])
        ).withClass("container-fluid")
    );

    return html(
        header(
            title(title),
            meta().attr("http-equiv", "Content-type")
                  .withContent("text/html")
                  .withCharset("utf-8"),
            meta().attr("http-equiv", "refresh").withContent("30"),
              //TODO: icon not working
            link().withRel("icon")
                  .withHref("/static/favicon-16x16.png")
                  .withType("image/png"),
            link().withRel("stylesheet")
                  .withHref("/static/bootstrap.min.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/vis.min.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/webui.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/timeline-view.css")
                  .withType("text/css"),
            link().withRel("stylesheet")
                  .withHref("/static/dag-viz.css")
                  .withType("text/css"),
            script().withSrc("/static/jquery-1.11.1.min.js"),
            script().withSrc("/static/vis.min.js"),
            script().withSrc("/static/bootstrap-tooltip.js"),
            script().withSrc("/static/initialize-tooltips.js"),
            script().withSrc("/static/webui.js"),
            script().withSrc("/static/d3.min.js"),
            script().withSrc("/static/dag-viz.js"),
            script().withSrc("/static/dagre-d3.min.js"),
            script().withSrc("/static/graphlib-dot.min.js")
        ),
        body(
            tags.toArray(new DomContent[0])
        )
    ).render();
  }
}
