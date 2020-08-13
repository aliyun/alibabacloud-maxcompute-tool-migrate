package com.aliyun.odps.datacarrier.taskscheduler.task;

import com.aliyun.odps.datacarrier.taskscheduler.DropRestoredTemporaryTableWorkItem;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

import java.util.Objects;

import static com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManagerDbImplUtils.*;

public class ObjectExportAndRestoreTask extends AbstractTask {
  private static final Logger LOG = LogManager.getLogger(ObjectExportAndRestoreTask.class);

  private final MetaSource.TableMetaModel tableMetaModel;
  private RestoreTaskInfo restoreTaskInfo;

  public ObjectExportAndRestoreTask(String id,
                                    MetaSource.TableMetaModel tableMetaModel,
                                    DirectedAcyclicGraph<Action, DefaultEdge> dag,
                                    MmaMetaManager mmaMetaManager) {
    super(id, dag, mmaMetaManager);
    this.tableMetaModel = Objects.requireNonNull(tableMetaModel);
    actionExecutionContext.setTableMetaModel(this.tableMetaModel);
  }

  public void setRestoreTaskInfo(RestoreTaskInfo restoreTaskInfo) {
    this.restoreTaskInfo = restoreTaskInfo;
  }

  public RestoreTaskInfo getRestoreTaskInfo() {
    return restoreTaskInfo;
  }

  @Override
  void updateMetadata() throws MmaException {
    if (TaskProgress.PENDING.equals(progress) || TaskProgress.RUNNING.equals(progress)) {
      return;
    }
    MmaMetaManager.MigrationStatus status = TaskProgress.SUCCEEDED.equals(progress) ?
        MmaMetaManager.MigrationStatus.SUCCEEDED :
        MmaMetaManager.MigrationStatus.FAILED;
    if (restoreTaskInfo != null) {
      mmaMetaManager.updateStatusInRestoreDB(restoreTaskInfo, status);
    } else {
      mmaMetaManager.updateStatus(tableMetaModel.databaseName,
          tableMetaModel.tableName,
          status);
    }
  }
}
