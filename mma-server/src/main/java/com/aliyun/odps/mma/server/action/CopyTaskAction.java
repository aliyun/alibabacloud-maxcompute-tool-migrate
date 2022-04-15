package com.aliyun.odps.mma.server.action;

import java.util.List;
import java.util.Calendar;
import java.util.stream.Collectors;

import com.aliyun.odps.Odps;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.mma.server.action.executor.ActionExecutorFactory;
import com.aliyun.odps.task.CopyTask;
import com.aliyun.odps.task.copy.LocalDatasource;
import com.aliyun.odps.task.copy.Datasource.Direction;
import com.aliyun.odps.mma.config.JobConfiguration;

import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.action.info.CopyTaskActionInfo;
import com.aliyun.odps.task.copy.TunnelDatasource;
import com.aliyun.oss.common.auth.HmacSHA1Signature;
import com.google.gson.JsonObject;
import org.apache.commons.codec.binary.Base64;

public abstract class CopyTaskAction extends AbstractAction<List<List<Object>>> {
  private static final String TOKEN_TYPE = "1";
  private static final String TOKEN_VERSION = "v1";
  private static final String TOKEN_ALGORITHM_ID = "1";

  // private static final String ODPS_COPY_STRIP_ENABLED = "odps.copy.strip.enabled";
  // compatible: 是否兼容odps新的数据类型，如，struct/array等
  private static final String ODPS_COPY_COMPATIBLE_ENABLED = "odps.copy.compatible.enabled";

  private final TableMetaModel source;
  private final TableMetaModel dest;
  private final ActionExecutionContext context;

  public CopyTaskAction(
          String id,
          TableMetaModel source,
          TableMetaModel dest,
          Task task,
          ActionExecutionContext actionExecutionContext) {
    super(id, task, actionExecutionContext);
    actionInfo = new CopyTaskActionInfo();
    this.source = source;
    this.dest = dest;
    this.context = actionExecutionContext;
  }

  @Override
  void executeInternal() throws Exception {
    future = ActionExecutorFactory.getCopyTaskExecutor().execute(
            getSrcOdps(),
            createCopyTask(getDestOdps(), Direction.EXPORT),
            id,
            (CopyTaskActionInfo) actionInfo
    );
  }

  public abstract Odps getSrcOdps();

  public abstract Odps getDestOdps();

  @Override
  void handleResult(List<List<Object>> result) {
    ((CopyTaskActionInfo) actionInfo).setResult(result);
  }

  @Override
  public Object getResult() {
    return ((CopyTaskActionInfo) actionInfo).getResult();
  }

  private CopyTask createCopyTask(Odps destOdps, Direction direction) throws MmaException {
    String partitions = getPartitions(source);
    TunnelDatasource tunnel = new TunnelDatasource(direction, dest.getDatabase(), dest.getTable(), partitions);
    String token = createToken(direction, dest.getDatabase(), dest.getTable());
    tunnel.setAccountType("token");
    tunnel.setSignature(token);
    tunnel.setOdpsEndPoint(destOdps.getEndpoint());

    LocalDatasource local = new LocalDatasource(direction, source.getDatabase(), source.getTable(), partitions);
    CopyTask task = new CopyTask("copy_task_" + Calendar.getInstance().getTimeInMillis());
    /**
     *  Append(-a)和Overwrite(-o)的语义很明确
     *  不过tunnel其实是只支持append操作
     *  所以overwrite模式只不过是帮你执行了一下alter table drop partition和add partition的操作。
     */
    task.setMode("Append");
    task.setLocalInfo(local);
    task.SetTunnelInfo(tunnel);

    JsonObject params = new JsonObject();
    params.addProperty(ODPS_COPY_COMPATIBLE_ENABLED, "false");
    // params.addProperty(ODPS_COPY_STRIP_ENABLED, "false");
    task.setProperty("settings", params.toString());

    return task;
  }

  private String createToken(Direction direction, String project, String table) throws MmaException {
    String grantee = "";
    String accessId = context.getConfig().get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_ID);
    String accessKey = context.getConfig().get(JobConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET);

    int day = 60 * 60 * 24;
    long expires = System.currentTimeMillis() / 1000 + day;

    String policy;
    String tableUrl = "projects/" + project + "/" + "tables/" + table;
    String projectUrl = "projects/" + project;
    String instanceUrl = "projects/" + project + "/instances/*";

    switch (direction.toString()) {
      case "EXPORT":
        policy = "{\"Version\":\"1\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"odps:Describe\",\"odps:Update\",\"odps:Alter\",\"odps:Drop\",\"odps:Create\"],\"Resource\":\"acs:odps:*:"
                + tableUrl.toLowerCase()
                + "\"},{\"Effect\":\"Allow\",\"Action\":[\"odps:CreateInstance\"],\"Resource\":\"acs:odps:*:"
                + projectUrl.toLowerCase()
                + "\"},{\"Effect\":\"Allow\",\"Action\":[\"odps:Read\"],\"Resource\":\"acs:odps:*:"
                + instanceUrl.toLowerCase() + "\"}]}";
        break;
      case "IMPORT":
        policy = "{\"Version\":\"1\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"odps:Describe\",\"odps:Select\",\"odps:Download\"],\"Resource\":\"acs:odps:*:"
                + tableUrl.toLowerCase() + "\"}]}";
        break;
      default:
        throw new MmaException("Unknown Direction mode!");
    }

    String data = TOKEN_ALGORITHM_ID + TOKEN_TYPE + accessId + grantee + expires + policy;
    HmacSHA1Signature signer = new HmacSHA1Signature();
    String signature = signer.computeSignature(accessKey, data) + "#" + TOKEN_ALGORITHM_ID + "#" + TOKEN_TYPE
            + "#" + accessId + "#" + grantee + "#" + expires + "#" + policy;
    String base64Signature = new String(Base64.encodeBase64(signature.getBytes()));

    return TOKEN_VERSION + "." + base64Signature;
  }

  private static String getPartitions(TableMetaModel tableMetaModel) {
    StringBuilder partitions = new StringBuilder();
    if (!tableMetaModel.getPartitionColumns().isEmpty()) {
      List<ColumnMetaModel> columnMetaModels = tableMetaModel.getPartitionColumns();
      List<PartitionMetaModel> partitionMetaModels = tableMetaModel.getPartitions();
      if (columnMetaModels.isEmpty() || partitionMetaModels.isEmpty()) {
        return partitions.toString();
      }

      List<String> partitionColumns = columnMetaModels.stream().map(ColumnMetaModel::getColumnName).collect(Collectors.toList());
      List<String> partitionValues = partitionMetaModels.get(0).getPartitionValues();
      for (int i = 0; i < partitionColumns.size(); i++) {
        partitions.append(partitionColumns.get(i)).append("=").append(partitionValues.get(i)).append(",");
      }
      return partitions.deleteCharAt(partitions.length() - 1).toString();
    }
    return partitions.toString();
  }

}
