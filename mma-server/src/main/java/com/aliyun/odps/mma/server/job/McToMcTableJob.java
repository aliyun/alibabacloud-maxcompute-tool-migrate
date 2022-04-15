package com.aliyun.odps.mma.server.job;

import java.util.List;
import java.util.LinkedList;
import java.util.Collections;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.server.meta.generated.JobRecord;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.server.task.TaskProgress;
import com.aliyun.odps.mma.server.task.McToMcTableSetUpTask;
import com.aliyun.odps.mma.server.task.McToMcTableDataTransmissionTask;
import com.aliyun.odps.mma.meta.transform.SchemaTransformerFactory;
import com.aliyun.odps.mma.meta.transform.SchemaTransformer.SchemaTransformResult;

public class McToMcTableJob extends AbstractTableJob {
  private static final Logger LOG = LogManager.getLogger(McToOssTableJob.class);

  McToMcTableJob(
          Job parentJob,
          JobRecord record,
          JobManager jobManager,
          MetaManager metaManager,
          MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  DirectedAcyclicGraph<Task, DefaultEdge> generateDag() throws Exception {
    LOG.info("Generate the Mc2Mc tableJob DAG, job id: {}", record.getJobId());

    try {
      MetaSource metaSource = metaSourceFactory.getMetaSource(config);

      TableMetaModel sourceMcTableMetaModel = metaSource.getTableMeta(
              config.get(JobConfiguration.SOURCE_CATALOG_NAME),
              config.get(JobConfiguration.SOURCE_OBJECT_NAME));

      SchemaTransformResult schemaTransformResult = SchemaTransformerFactory
              .get(DataSourceType.MaxCompute)
              .transform(sourceMcTableMetaModel, config);
      TableMetaModel sinkMcTableMetaModel = schemaTransformResult.getTableMetaModel();

      List<Job> pendingSubJobs = null;
      if (!sourceMcTableMetaModel.getPartitionColumns().isEmpty()) {
        pendingSubJobs = jobManager.listSubJobsByStatus(this, JobStatus.PENDING);
      }

      DirectedAcyclicGraph<Task, DefaultEdge> dag = new DirectedAcyclicGraph<>(DefaultEdge.class);
      Task setUpTask = getSetUpTask(
              metaSource,
              sourceMcTableMetaModel,
              sinkMcTableMetaModel,
              pendingSubJobs);
      List<Task> dataTransmissionTasks = getDataTransmissionTasks(
              metaSource,
              sourceMcTableMetaModel,
              sinkMcTableMetaModel,
              pendingSubJobs);

      dag.addVertex(setUpTask);
      dataTransmissionTasks.forEach(dag::addVertex);
      dataTransmissionTasks.forEach(t -> dag.addEdge(setUpTask, t));
      return dag;
    } catch (Exception e) {
      String stackTrace = ExceptionUtils.getFullStackTrace(e);
      fail(stackTrace);
      throw e;
    }

  }

  private Task getSetUpTask(MetaSource metaSource,
                            TableMetaModel sourceMcTableMetaModel,
                            TableMetaModel destMcTableMetaModel,
                            List<Job> pendingSubJobs) throws Exception {
    List<TableMetaModel> groupDests = null;
    if (!sourceMcTableMetaModel.getPartitionColumns().isEmpty()) {
      groupDests = getStaticTablePartitionGroups(
              metaSource,
              sourceMcTableMetaModel,
              destMcTableMetaModel,
              pendingSubJobs)
              .stream()
              .map(TablePartitionGroup::getDest)
              .collect(Collectors.toList());
    }
    String taskIdPrefix = generateTaskIdPrefix();
    return new McToMcTableSetUpTask(
            taskIdPrefix + ".SetUp",
            getRootJobId(),
            config,
            destMcTableMetaModel,
            groupDests,
            this);
  }

  private List<Task> getDataTransmissionTasks(MetaSource metaSource,
                                              TableMetaModel sourceMcTableMetaModel,
                                              TableMetaModel destMcTableMetaModel,
                                              List<Job> pendingSubJobs) throws Exception {
    List<Task> ret = new LinkedList<>();
    boolean isPartitioned = !sourceMcTableMetaModel.getPartitionColumns().isEmpty();
    String taskIdPrefix = generateTaskIdPrefix();
    String rootJobId = getRootJobId();

    if (isPartitioned) {
      List<TablePartitionGroup> groups = getTablePartitionGroups(
              metaSource,
              sourceMcTableMetaModel,
              destMcTableMetaModel,
              pendingSubJobs);

      for (int i = 0; i < groups.size(); i++) {
        String taskId = taskIdPrefix + ".DataTransmission" + ".part." + i;
        Task task = new McToMcTableDataTransmissionTask(
                taskId,
                rootJobId,
                config,
                groups.get(i).getSource(),
                groups.get(i).getDest(),
                this,
                groups.get(i).getJobs());
        LOG.info(
                "McToMcTableJob data transmission tasks generated, id: {}, rootJobId: {}, jobs: {}",
                taskId,
                rootJobId,
                groups.get(i).getJobs().stream().map(Job::getId).collect(Collectors.toList()));
        ret.add(task);
      }
    } else {
      Task task = new McToMcTableDataTransmissionTask(
              taskIdPrefix + ".DataTransmission",
              rootJobId,
              config,
              sourceMcTableMetaModel,
              destMcTableMetaModel,
              this,
              Collections.emptyList());
      ret.add(task);
    }

    return ret;
  }

  List<TablePartitionGroup> getTablePartitionGroups(
          MetaSource metaSource,
          TableMetaModel source,
          TableMetaModel dest,
          List<Job> pendingSubJobs) throws Exception {

    if (source.getPartitionColumns().isEmpty() || pendingSubJobs.isEmpty()) {
      // Not a partitioned table or the number of partitions to transfer is zero
      LOG.info(
              "McToMcTableJob create partition group, database: {}, table: {}, is partitioned: {}, num partitions: {}",
              source.getDatabase(),
              source.getTable(),
              !source.getPartitionColumns().isEmpty(),
              pendingSubJobs.size());
      return Collections.singletonList(
              new TablePartitionGroup(source, dest, Collections.singletonList(this)));
    }

    return generateAdaptiveTablePartitionGroups(metaSource, source, dest, pendingSubJobs);

  }

  private List<TablePartitionGroup> generateAdaptiveTablePartitionGroups(
          MetaSource metaSource,
          TableMetaModel source,
          TableMetaModel dest,
          List<Job> pendingSubJobs) throws Exception {

    LOG.info("McToMcTableJob catalog: {}, table: {}, enter getAdaptiveTablePartitionGroups",
            source.getDatabase(), source.getTable());

    if (source.getPartitionColumns().isEmpty()) {
      LOG.info("McToMcTableJob database: {}, table: {}, non-partitioned table is not supported",
              source.getDatabase(), source.getTable());
      return null;
    }

    List<TablePartitionGroup> ret = new LinkedList<>();
    for (Job job : pendingSubJobs) {
      List<String> partitionVals = ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(
              job.getJobConfiguration().get(JobConfiguration.SOURCE_OBJECT_NAME));
      // TODO: The source table metadata already contains the partition metadata
      PartitionMetaModel partitionMetaModel = metaSource.getPartitionMeta(
              source.getDatabase(), source.getTable(), partitionVals);
      // Make sure that the size of each partition is valid.
      if (partitionMetaModel.getSize() == null) {
        LOG.info(
                "McToMcTableJob database: {}, table: {}, partition: {}, size is not valid",
                source.getDatabase(),
                source.getTable(),
                partitionMetaModel.getPartitionValues());
        // Tips: different from others.
        continue;
      }
      TableMetaModel.TableMetaModelBuilder sourceBuilder = new TableMetaModel.TableMetaModelBuilder(source);
      TableMetaModel.TableMetaModelBuilder destBuilder = new TableMetaModel.TableMetaModelBuilder(dest);
      LOG.info("McToMcTableJob Database: {}, table: {}, partition: {}",
              source.getDatabase(),
              source.getTable(),
              partitionMetaModel.getPartitionValues());
      sourceBuilder.partitions(Collections.singletonList(partitionMetaModel));
      destBuilder.partitions(Collections.singletonList(partitionMetaModel));
      ret.add(
              new TablePartitionGroup(
                      sourceBuilder.build(),
                      destBuilder.build(),
                      Collections.singletonList(job)));
    }
    return ret;
  }

  @Override
  public synchronized void setStatus(Task task) {

    if (JobStatus.SUCCEEDED.equals(getStatus())
            || JobStatus.FAILED.equals(getStatus())
            || JobStatus.CANCELED.equals(getStatus())) {
      LOG.info("Job has terminated, id: {}, status: {}, task id: {}, task status: {}",
              record.getJobId(),
              getStatus(),
              task.getId(),
              task.getProgress());
    }

    TaskProgress taskStatus = task.getProgress();

    switch (taskStatus) {
      case SUCCEEDED:
        if (task instanceof McToMcTableDataTransmissionTask) {
          handleDataTransmissionTask((McToMcTableDataTransmissionTask) task);
        }
        if (dag.vertexSet()
                .stream()
                .filter(t -> t instanceof McToMcTableDataTransmissionTask)
                .allMatch(t -> TaskProgress.SUCCEEDED.equals(t.getProgress()))) {
          setStatusInternal(JobStatus.SUCCEEDED);
        }
        break;
      case FAILED:
        if (task instanceof McToMcTableDataTransmissionTask) {
          handleDataTransmissionTask((McToMcTableDataTransmissionTask) task);
        } else {
          String reason = String.format("%s failed, id: %s", task.getClass(), task.getId());
          fail(reason);
        }
        break;
      case RUNNING:
        LOG.info("Job running, id: {}", record.getJobId());
        setStatusInternal(JobStatus.RUNNING);
        break;
      default:
    }

  }

}
