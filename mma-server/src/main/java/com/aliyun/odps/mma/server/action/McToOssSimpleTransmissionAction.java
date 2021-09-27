package com.aliyun.odps.mma.server.action;


import java.io.InputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datanucleus.util.StringUtils;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Function;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MmaConfig.OssConfig;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.OssUtils;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;

public class McToOssSimpleTransmissionAction extends DefaultAction {

  private static final Logger LOG = LogManager.getLogger(McToOssSimpleTransmissionAction.class);
  private final AbstractConfiguration config;
  private final ObjectType objectType;
  private final OssConfig ossConfig;
  private final Odps odps;
  private final String metafile;
  private final String datafile;

  public McToOssSimpleTransmissionAction(
      String id,
      Odps odps,
      AbstractConfiguration config,
      ObjectType objectType,
      OssConfig ossConfig,
      String metafile,
      String datafile,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.config = config;
    this.objectType = objectType;
    this.ossConfig = ossConfig;
    this.odps = odps;
    this.metafile = metafile;
    this.datafile = datafile;
  }


  @Override
  void handleResult(Object result) {}

  @Override
  public String getName() {
    return objectType.name() + " transmission";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public Object call() throws Exception {
    Object object = OdpsUtils.getObject(
        odps,
        config.get(JobConfiguration.SOURCE_CATALOG_NAME),
        config.get(JobConfiguration.SOURCE_OBJECT_NAME),
        objectType);

    if (ObjectType.RESOURCE.equals(objectType)) {
      resourceTransmission(object);
    } else if(ObjectType.FUNCTION.equals(objectType)) {
      functionTransmission(object);
    }
    return null;
  }

  private void resourceTransmission(Object object) throws OdpsException, MmaException {
    Resource resource = (Resource) object;
    if (StringUtils.isEmpty(resource.getName())) {
      LOG.error("Invalid resource name {} for task {}", resource.getName(), id);
      throw new MmaException("ERROR: Resource name is empty");
    }

    OdpsResourceInfo resourceInfo = new OdpsResourceInfo(resource);
    String content = GsonUtils.GSON.toJson(resourceInfo);
    LOG.info("Action: {}, resource info: {}", id, content);
    OssUtils.createFile(ossConfig, metafile, content);

    if(!Resource.Type.TABLE.equals(resourceInfo.getType())){
      FileResource fileResource = (FileResource) resource;
      InputStream stream = odps.resources().getResourceAsStream(fileResource.getProject(), fileResource.getName());
      OssUtils.createFile(ossConfig, datafile, stream);
    }
  }

  private void functionTransmission(Object object) {
    Function function = (Function)  object;
    OdpsFunctionInfo functionInfo = new OdpsFunctionInfo(function);
    String content = GsonUtils.GSON.toJson(functionInfo);
    LOG.info("Action: {}, function info: {}", id, content);
    OssUtils.createFile(ossConfig, metafile, content);
  }
}
