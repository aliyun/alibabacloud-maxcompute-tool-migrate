package com.aliyun.odps.mma.server.action;


import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class OssToMcSimpleTransmissionAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(OssToMcSimpleTransmissionAction.class);
  private final OssConfig ossConfig;
  private final ObjectType objectType;
  private final String metafile;
  private final String datafile;
  private final Odps odps;
  private final boolean update;

  public OssToMcSimpleTransmissionAction(
      String id,
      OssConfig ossConfig,
      ObjectType objectType,
      String metafile,
      String datafile,
      Odps odps,
      boolean update,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.ossConfig = ossConfig;
    this.objectType = objectType;
    this.metafile = metafile;
    this.datafile = datafile;
    this.odps = odps;
    this.update = update;
  }


  @Override
  void handleResult(Object result) {
  }

  @Override
  public String getName() {
    return objectType.name() + " restoration";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() throws Exception {
    switch (objectType) {
      case RESOURCE:
        resource();
        break;
      case FUNCTION:
        function();
        break;
      default:
        break;
    }

    return null;
  }

  private void checkFileExists(String filename) throws MmaException {
    if (!OssUtils.exists(ossConfig, metafile)) {
      throw new MmaException(String.format("ActionId: %s, OSS file %s not found", id, filename));
    }
  }

  private void resource() throws IOException, MmaException, OdpsException {
    // todo getFullConfigGson()
    checkFileExists(metafile);
    String content = OssUtils.readFile(ossConfig, metafile);
    OdpsResourceInfo resourceInfo = GsonUtils.GSON.fromJson(content, OdpsResourceInfo.class);
    Resource resource = resourceInfo.toResource();

    if (Resource.Type.TABLE.equals(resource.getType())) {
      //todo update??
      OdpsUtils.addTableResource(odps, odps.getDefaultProject(), (TableResource) resource, update);
    } else {
      checkFileExists(datafile);
      // todo jobid?
      String localFilePath = OssUtils.downloadFile(ossConfig, id, datafile);
      OdpsUtils.addFileResource(
          odps, odps.getDefaultProject(),
          (FileResource) resource, localFilePath,
          update, true);
    }
    LOG.info("Restore resource {} succeed", content);
  }

  private void function() throws IOException, OdpsException, MmaException {
    checkFileExists(metafile);
    String content = OssUtils.readFile(ossConfig, metafile);
    // todo getFullConfigGson()
    OdpsFunctionInfo functionInfo = GsonUtils.GSON.fromJson(content, OdpsFunctionInfo.class);
    OdpsUtils.createFunction(odps, odps.getDefaultProject(), functionInfo, update);

    LOG.info("Restore function {} succeed", content);
  }
}
