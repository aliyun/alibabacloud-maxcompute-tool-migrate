package com.aliyun.odps.datacarrier.taskscheduler.task;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.action.Action;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MmaMetaManager;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;

public class DropTemporaryTableTask extends AbstractTask {

  private String db;
  private String tbl;

  public DropTemporaryTableTask(String id, DirectedAcyclicGraph<Action, DefaultEdge> dag, MmaMetaManager mmaMetaManager,
                                String db, String tbl) {
    super(id, dag, mmaMetaManager);
    this.db = db;
    this.tbl = tbl;
  }

  @Override
  void updateMetadata() throws MmaException {
    if (TaskProgress.SUCCEEDED.equals(progress)) {
      mmaMetaManager.removeTemporaryTableMeta(getOriginId(), db, tbl);
    }
  }
}
