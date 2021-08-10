package com.aliyun.odps.mma.server.job;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.server.meta.generated.Job.JobBuilder;
import com.aliyun.odps.mma.server.task.Task;

public class PartitionJob extends AbstractJob {

  public PartitionJob(
      Job parentJob,
      com.aliyun.odps.mma.server.meta.generated.Job record,
      JobManager jobManager,
      MetaManager metaManager,
      MetaSourceFactory metaSourceFactory) {
    super(parentJob, record, jobManager, metaManager, metaSourceFactory);
  }

  @Override
  public synchronized void setStatus(Task task) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Task> getExecutableTasks() {
    throw new UnsupportedOperationException();
  }

  @Override
  boolean updateObjectMetadata() throws Exception {
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    String catalogName = config.get(JobConfiguration.SOURCE_CATALOG_NAME);
    String partitionIdentifier = config.get(JobConfiguration.SOURCE_OBJECT_NAME);
    String tableName = ConfigurationUtils.getTableNameFromPartitionIdentifier(partitionIdentifier);
    List<String> partitionValues =
        ConfigurationUtils.getPartitionValuesFromPartitionIdentifier(partitionIdentifier);
    PartitionMetaModel partitionMetaModel = metaSource.getPartitionMeta(
        catalogName,
        tableName,
        partitionValues);

    Long oldObjectLastModifiedTime =
        config.containsKey(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME) ?
            Long.valueOf(config.get(JobConfiguration.SOURCE_OBJECT_LAST_MODIFIED_TIME)) : null;
    Long newObjectLastModifiedTime = partitionMetaModel.getLastModificationTime();
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
}
