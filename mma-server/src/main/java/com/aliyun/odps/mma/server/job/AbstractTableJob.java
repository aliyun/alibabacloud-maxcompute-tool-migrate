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

package com.aliyun.odps.mma.server.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.Constants;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.job.utils.JobUtils;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.model.PartitionMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel.TableMetaModelBuilder;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.generated.Job.JobBuilder;
import com.aliyun.odps.mma.server.task.TableDataTransmissionTask;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;

public abstract class AbstractTableJob extends AbstractJob {

  private static final Logger LOG = LogManager.getLogger(AbstractTableJob.class);

  public static class TablePartitionGroup {
    private TableMetaModel source;
    private TableMetaModel dest;
    private List<Job> jobs;

    TablePartitionGroup(TableMetaModel source, TableMetaModel dest, List<Job> jobs) {
      this.source = Validate.notNull(source);
      this.dest = Validate.notNull(dest);
      this.jobs = Validate.notNull(jobs);
    }

    public TableMetaModel getSource() {
      return source;
    }

    public TableMetaModel getDest() {
      return dest;
    }

    public List<Job> getJobs() {
      return jobs;
    }
  }

  DirectedAcyclicGraph<Task, DefaultEdge> dag;

  AbstractTableJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public synchronized List<Task> getTasks() {
    if (dag != null) {
      return new ArrayList<>(dag.vertexSet());
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public synchronized List<Task> getExecutableTasks() {
    if (dag == null) {
      LOG.info("The DAG is not generated yet, job id: {}", record.getJobId());
      try {
        dag = generateDag();
      } catch (Exception e) {
        return Collections.emptyList();
      }
    }

    List<Task> ret = new LinkedList<>();
    for (Task t : dag.vertexSet()) {
      boolean executable = TaskProgress.PENDING.equals(t.getProgress())
          && Graphs.predecessorListOf(dag, t)
                   .stream()
                   .allMatch(p -> TaskProgress.SUCCEEDED.equals(p.getProgress()));
      if (executable) {
        ret.add(t);
      }
    }

    if (ret.size() > 0) {
      LOG.info("Executable tasks, job id: {}, ret: {}", record.getJobId(), ret);
    }
    return ret;
  }

  @Override
  synchronized void fail(String reason) {
    super.fail(reason);
    if (dag == null) {
      LOG.info("The DAG is not generated yet, job id: {}", record.getJobId());
      return;
    }
    dag.vertexSet().forEach(t -> {
      try {
        LOG.info(
            "Stop task due to job failure, job id: {}, task id: {}",
            record.getJobId(),
            t.getId());
        t.stop();
      } catch (MmaException e) {
        LOG.warn(
            "Failed to stop task, task id: {}, reason: {}",
            t.getId(),
            ExceptionUtils.getFullStackTrace(e));
      }
    });
  }

  @Override
  public synchronized boolean retry() {
    boolean retry = super.retry();
    if (retry) {
      try {
        dag = generateDag();
      } catch (Exception e) {
        return false;
      }
    }
    return retry;
  }

  @Override
  public synchronized boolean reset(boolean force) throws Exception {
    boolean reset = super.reset(force);
    if (reset) {
      dag = null;
    }
    return reset;
  }

  @Override
  boolean updateObjectMetadata() throws Exception {
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String tableName = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    TableMetaModel tableMetaModel = metaSource.getTableMeta(catalogName, tableName);

    Long oldObjectLastModifiedTime =
        config.containsKey(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME) ?
            Long.valueOf(config.get(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME)) : null;
    Long newObjectLastModifiedTime = tableMetaModel.getLastModificationTime();
    boolean objectChanged = oldObjectLastModifiedTime != null
        && newObjectLastModifiedTime != null
        && oldObjectLastModifiedTime < newObjectLastModifiedTime;

    if (objectChanged) {
      JobBuilder jobBuilder = new JobBuilder(record);
      Map<String, String> jobConfBuilder = new HashMap<>(config);
      jobConfBuilder.put(
          JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME,
          Long.toString(newObjectLastModifiedTime));
      JobConfiguration newJobConf = new JobConfiguration(jobConfBuilder);
      jobBuilder.jobConfig(newJobConf.toString());
      config = newJobConf;
      update(jobBuilder);
      return true;
    }
    return false;
  }

  @Override
  void removeInvalidSubJobs() throws Exception {
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String tableName = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    TableMetaModel tableMetaModel = metaSource.getTableMeta(catalogName, tableName);
    if (!tableMetaModel.getPartitionColumns().isEmpty()) {
      // Find invalid sub jobs
      Map<String, Long> partitionIdentifierToLastModificationTime = tableMetaModel
          .getPartitions()
          .stream()
          .collect(
              Collectors.toMap(
                  p -> ConfigurationUtils.toPartitionIdentifier(tableName, p.getPartitionValues()),
                  PartitionMetaModel::getLastModificationTime));
      for (Job subJob : getSubJobs()) {
        String partitionIdentifier =
            subJob.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME);
        if (!partitionIdentifierToLastModificationTime.containsKey(partitionIdentifier)) {
          LOG.info(
              "Remove invalid sub job, parent job id: {}, sub job id: {}",
              record.getJobId(),
              subJob.getId());
          jobManager.removeSubJob(record.getJobId(), subJob.getId());
        }
      }
    }
  }

  @Override
  boolean addNewSubJobs() throws Exception {
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String tableName = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    TableMetaModel tableMetaModel = metaSource.getTableMeta(catalogName, tableName);

    boolean hasNewSubJob = false;
    if (!tableMetaModel.getPartitionColumns().isEmpty()) {
      JobUtils.PartitionFilter partitionFilter = new JobUtils.PartitionFilter(config);
      Map<String, Long> partitionIdentifierToLastModificationTime = tableMetaModel
          .getPartitions()
          .stream()
          .filter(p -> partitionFilter.filter(p.getPartitionValues()))
          .collect(
              Collectors.toMap(
                  p -> ConfigurationUtils.toPartitionIdentifier(tableName, p.getPartitionValues()),
                  PartitionMetaModel::getLastModificationTime));
      // Find new sub jobs
      Set<String> currentPartitionIdentifierSet = getSubJobs()
          .stream()
          .map(j -> j.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME))
          .collect(Collectors.toSet());
      for (Entry<String, Long> entry: partitionIdentifierToLastModificationTime.entrySet()) {
        if (!currentPartitionIdentifierSet.contains(entry.getKey())) {
          String subJobId = JobUtils.generateJobId(true);
          Map<String, String> subConfig = new HashMap<>(config);
          subConfig.put(JobConfiguration.JOB_ID, subJobId);
          subConfig.put(JobConfiguration.SOURCE_OBJECT_NAME, entry.getKey());
          subConfig.put(JobConfiguration.DEST_OBJECT_NAME, entry.getKey());
          subConfig.put(JobConfiguration.OBJECT_TYPE, ObjectType.PARTITION.name());
          if (entry.getValue() != null) {
            subConfig.put(
                JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME,
                Long.toString(entry.getValue()));
          }
          jobManager.addSubJob(record.getJobId(), new JobConfiguration(subConfig));
          hasNewSubJob = true;
          LOG.info(
              "New sub job, parent job id: {}, sub job id: {}",
              record.getJobId(),
              subJobId);
        }
      }
    }
    return hasNewSubJob;
  }

  @Override
  public synchronized void stop() throws MmaException {
    super.stop();
    if (dag != null) {
      for (Task t : dag.vertexSet()) {
        t.stop();
      }
    }
  }

  @Override
  public String toString() {
    String basics = super.toString();
    if (dag != null) {
      String extended = dag
          .vertexSet()
          .stream()
          .map(t -> t.getId() + ", " +  t.getProgress().name())
          .collect(Collectors.joining(", "));
      return "basics: " + basics + ", " + "extended: " + extended;
    }
    return "basics: " + basics;
  }

  abstract DirectedAcyclicGraph<Task, DefaultEdge> generateDag() throws Exception;

  List<TablePartitionGroup> getTablePartitionGroups(
      MetaSource metaSource,
      TableMetaModel source,
      TableMetaModel dest,
      List<Job> pendingSubJobs) throws Exception {

    if (source.getPartitionColumns().isEmpty() || pendingSubJobs.isEmpty()) {
      // Not a partitioned table or the number of partitions to transfer is zero
      LOG.info(
          "Create partition group, database: {}, table: {}, is partitioned: {}, num partitions: {}",
          source.getDatabase(),
          source.getTable(),
          !source.getPartitionColumns().isEmpty(),
          pendingSubJobs.size());
      return Collections.singletonList(
          new TablePartitionGroup(source, dest, Collections.singletonList(this)));
    }

    List<TablePartitionGroup> groups =
        generateAdaptiveTablePartitionGroups(metaSource, source, dest, pendingSubJobs);
    if (groups != null) {
      return groups;
    }

    LOG.info("Generate adaptive table partition groups failed, ");
    return getStaticTablePartitionGroups(metaSource, source, dest, pendingSubJobs);
  }

  /**
   * Generate partition groups. For each partition group, the number of partition it contains
   * will not exceed {@link Constants#MAX_PARTITION_GROUP_SIZE} and the
   * data size will not exceed {@link JobConfiguration#TABLE_PARTITION_GROUP_SPLIT_SIZE}.
   *
   * If {@link JobConfiguration#TABLE_PARTITION_GROUP_SPLIT_SIZE} is not configured, a default value
   * {@link JobConfiguration#TABLE_PARTITION_GROUP_SPLIT_SIZE_DEFAULT_VALUE} will be used.
   *
   * @return List of partition groups, or null if the metadata doesn't contain the data size.
   */
  private List<TablePartitionGroup> generateAdaptiveTablePartitionGroups(
      MetaSource metaSource,
      TableMetaModel source,
      TableMetaModel dest,
      List<Job> pendingSubJobs) throws Exception {

    LOG.info("Catalog: {}, table: {}, enter getAdaptiveTablePartitionGroups",
             source.getDatabase(), source.getTable());

    if (source.getPartitionColumns().isEmpty()) {
      LOG.info("Database: {}, table: {}, non-partitioned table is not supported",
               source.getDatabase(), source.getTable());
      return null;
    }

    List<PartitionMetaModel> partitionMetaModels = new LinkedList<>();
    for (Job job : pendingSubJobs) {
      List<String> partitionVals = ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(
          job.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME));
      // TODO: The source table metadata already contains the partition metadata
      PartitionMetaModel partitionMetaModel = metaSource.getPartitionMeta(
          source.getDatabase(), source.getTable(), partitionVals);
      partitionMetaModels.add(partitionMetaModel);
    }

    // Make sure that the size of each partition is valid.
    for (PartitionMetaModel p : partitionMetaModels) {
      if (p.getSize() == null) {
        LOG.info(
            "Database: {}, table: {}, partition: {}, size is not valid",
            source.getDatabase(),
            source.getTable(),
            p.getPartitionValues());
        return null;
      }
    }

    List<TablePartitionGroup> ret = new LinkedList<>();

    int groupDataSizeInGigaByte = Integer.valueOf(config.getOrDefault(
        JobConfiguration.TABLE_PARTITION_GROUP_SPLIT_SIZE,
        JobConfiguration.TABLE_PARTITION_GROUP_SPLIT_SIZE_DEFAULT_VALUE));
    long groupDataSizeInByte = groupDataSizeInGigaByte * 1024 * 1024 * 1024L;
    int groupNumOfPartitionLimit = Integer.valueOf(config.getOrDefault(
        JobConfiguration.TABLE_PARTITION_GROUP_SIZE,
        JobConfiguration.TABLE_PARTITION_GROUP_SIZE_DEFAULT_VALUE));

    // Sort by size in descending order
    partitionMetaModels.sort((o1, o2) -> {
      if (o1.getSize() > o2.getSize()) {
        return -1;
      } else if (o1.getSize().equals(o2.getSize())) {
        return 0;
      } else {
        return 1;
      }
    });

    int i = 0;
    while (i < partitionMetaModels.size()) {
      TableMetaModelBuilder sourceBuilder = new TableMetaModelBuilder(source);
      TableMetaModelBuilder destBuilder = new TableMetaModelBuilder(dest);
      // Handle extremely large partitions. Generate a task for each of them.
      if (partitionMetaModels.get(i).getSize() > groupDataSizeInByte) {
        LOG.info("Database: {}, table: {}, partition: {}, size exceeds split size: {}",
                 source.getDatabase(),
                 source.getTable(),
                 partitionMetaModels.get(i).getPartitionValues(),
                 partitionMetaModels.get(i).getSize());
        sourceBuilder.partitions(Collections.singletonList(partitionMetaModels.get(i)));
        destBuilder.partitions(Collections.singletonList(partitionMetaModels.get(i)));
        ret.add(
            new TablePartitionGroup(
                sourceBuilder.build(),
                destBuilder.build(),
                Collections.singletonList(pendingSubJobs.get(i))));
        i++;
        continue;
      }

      List<PartitionMetaModel> currentPartitionGroup = new LinkedList<>();
      List<Job> jobs = new LinkedList<>();
      LOG.info("Database: {}, table: {}, assemble {} th partition group",
                source.getDatabase(), source.getTable(), ret.size());
      long sum = 0L;
      // Keep adding partitions until the total size exceeds splitSizeInByte or the number of
      // partitions exceeds groupNumOfPartitionLimit
      while (i < partitionMetaModels.size()
          && currentPartitionGroup.size() < groupNumOfPartitionLimit) {
        if (sum + partitionMetaModels.get(i).getSize() <= groupDataSizeInByte) {
          currentPartitionGroup.add(partitionMetaModels.get(i));
          jobs.add(pendingSubJobs.get(i));
          LOG.info(
              "Database: {}, table: {}, add partition: {}, size: {} to partition group",
              source.getDatabase(),
              source.getTable(),
              partitionMetaModels.get(i).getPartitionValues(),
              partitionMetaModels.get(i).getSize());
          sum += partitionMetaModels.get(i).getSize();
        } else {
          break;
        }
        i++;
      }
      sourceBuilder.partitions(currentPartitionGroup);
      destBuilder.partitions(currentPartitionGroup);

      LOG.info(
          "Database: {}, table: {}, {} th partition group, num partitions: {}, size: {}",
          source.getDatabase(),
          source.getTable(),
          ret.size(),
          currentPartitionGroup.size(),
          sum);
      ret.add(new TablePartitionGroup(sourceBuilder.build(), destBuilder.build(), jobs));
    }

    return ret;
  }

  /**
   * Generate partition groups. For each group, it contains at most
   * {@link JobConfiguration#TABLE_PARTITION_GROUP_SIZE} partitions.
   *
   * If {@link JobConfiguration#TABLE_PARTITION_GROUP_SIZE} is not configured, a default value
   * {@link JobConfiguration#TABLE_PARTITION_GROUP_SIZE_DEFAULT_VALUE} will be used.
   *
   * @return List of partition groups
   */
  List<TablePartitionGroup> getStaticTablePartitionGroups(
      MetaSource metaSource,
      TableMetaModel source,
      TableMetaModel dest,
      List<Job> pendingSubJobs) throws Exception {

    LOG.info("Database: {}, table: {}, enter getStaticTablePartitionGroups",
             source.getDatabase(), source.getTable());

    List<TablePartitionGroup> ret = new LinkedList<>();
    int groupNumOfPartitionLimit = Integer.valueOf(config.getOrDefault(
        JobConfiguration.TABLE_PARTITION_GROUP_SIZE,
        JobConfiguration.TABLE_PARTITION_GROUP_SIZE_DEFAULT_VALUE));

    List<PartitionMetaModel> partitionMetaModels = new ArrayList<>(pendingSubJobs.size());
    for (Job job : pendingSubJobs) {
      List<String> partitionVals = ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(
          job.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME));
      PartitionMetaModel partitionMetaModel = metaSource.getPartitionMeta(
          source.getDatabase(), source.getTable(), partitionVals);
      partitionMetaModels.add(partitionMetaModel);
    }

    int startIdx = 0;
    while (startIdx < partitionMetaModels.size()) {
      TableMetaModelBuilder sourceBuilder = new TableMetaModelBuilder(source);
      TableMetaModelBuilder destBuilder = new TableMetaModelBuilder(dest);
      // Set partitions
      int endIdx = Math.min(partitionMetaModels.size(), startIdx + groupNumOfPartitionLimit);
      List<PartitionMetaModel> currentPartitionGroup =
          new ArrayList<>(partitionMetaModels.subList(startIdx, endIdx));
      sourceBuilder.partitions(currentPartitionGroup);
      destBuilder.partitions(currentPartitionGroup);
      ret.add(
          new TablePartitionGroup(
              sourceBuilder.build(),
              destBuilder.build(),
              pendingSubJobs.subList(startIdx, endIdx)));
      startIdx += groupNumOfPartitionLimit;
    }

    return ret;
  }

  <T extends TableDataTransmissionTask> void handleDataTransmissionTask(T task) {
    String taskId = task.getId();
    if (dag.vertexSet().stream().noneMatch(t -> taskId.equals(t.getId()))) {
      LOG.info("Outdated task found, job id: {}, task id: {}", record.getJobId(), taskId);
      return;
    }

    // Update sub job status
    if (TaskProgress.SUCCEEDED.equals(task.getProgress())) {
      for (Job job : task.getSubJobs()) {
        ((AbstractJob) job).setStatusInternal(JobStatus.SUCCEEDED);
      }
    } else {
      for (Job job : task.getSubJobs()) {
        ((AbstractJob) job).setStatusInternal(JobStatus.FAILED);
      }
    }

    long numOfDataTransmissionTasks = dag
        .vertexSet()
        .stream()
        .filter(t -> task.getClass().isInstance(t)).count();
    long numOfTerminatedDataTransmissionTasks = dag
        .vertexSet()
        .stream()
        .filter(t -> task.getClass().isInstance(t))
        .filter(t -> TaskProgress.FAILED.equals(t.getProgress())
            || TaskProgress.SUCCEEDED.equals(t.getProgress())
            || TaskProgress.CANCELED.equals(t.getProgress()))
        .count();
    List<Task> failedDataTransmissionTasks = dag
        .vertexSet()
        .stream()
        .filter(t -> task.getClass().isInstance(t))
        .filter(t -> TaskProgress.FAILED.equals(t.getProgress()))
        .collect(Collectors.toList());

    LOG.info(
        "Job running, id: {}, num of data trans tasks: {}, terminated: {}, failed: {}",
        record.getJobId(),
        numOfDataTransmissionTasks,
        numOfTerminatedDataTransmissionTasks,
        failedDataTransmissionTasks.size());

    if (numOfDataTransmissionTasks == numOfTerminatedDataTransmissionTasks
        && !failedDataTransmissionTasks.isEmpty()) {
      String taskIds = failedDataTransmissionTasks
          .stream()
          .map(Task::getId).collect(Collectors.joining(","));
      String reason = String.format(
          "%s failed, id(s): %s",
          task.getClass().getName(),
          taskIds);
      fail(reason);
    }
  }
}
