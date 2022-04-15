package com.aliyun.odps.mma.server.action;

import java.util.List;
import java.util.ArrayList;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.aliyun.odps.FileResource;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.mma.server.OdpsUtils;
import com.aliyun.odps.mma.server.action.info.DefaultActionInfo;
import com.aliyun.odps.mma.server.task.Task;
import com.aliyun.odps.mma.util.GsonUtils;
import com.aliyun.odps.utils.StringUtils;


public class McToMcResourceAction extends DefaultAction {
  private static final Logger LOG = LogManager.getLogger(McToMcResourceAction.class);

  private final JobConfiguration config;
  private final Odps srcOdps;
  private final Odps destOdps;
  private final String destProject;

  private final Result result = new Result();

  public McToMcResourceAction(String id, Task task, ActionExecutionContext context,
                              JobConfiguration config, Odps srcOdps, Odps destOdps) {
    super(id, task, context);
    this.config = config;
    this.srcOdps = srcOdps;
    this.destOdps = destOdps;
    this.destProject = destOdps.getDefaultProject();
  }

  @Override
  void handleResult(Object result) {
    ((DefaultActionInfo) actionInfo).setResult(result.toString());
  }

  @Override
  public String getName() {
    return "McToMC Resources transmission";
  }

  @Override
  public Object getResult() {
    /**
     * Success:
     *    status.1 success.size + exist.size + invalid.size == all.size (normal)
     * Failed:
     *    status.1 success.size + exist.size + invalid.size != all.size (abnormal)
     *    status.2 exists.size + invalid.size == all.size (abnormal)
     *      status 2.1 sync.size == 0
     */
    if (ActionProgress.FAILED.equals(getProgress())) {
      if (!result.successExistInvalidAll()) {
        // For specified resource which has sync failed.
        if (result.getAll().size() == 1) {
          result.setFailed(result.getAll());
          result.setReason(getReason());
        } else {
          List<String> var1 = new ArrayList<>(result.getAll());
          List<String> var2 = new ArrayList<>(result.getSuccess());
          var2.addAll(result.getExist());
          var2.addAll(result.getInvalid());
          var1.removeAll(var2);
          result.setFailed(var1);
          result.setReason(String.format(
                  "Maybe the first resource has sync failed which in the failed list, detail: %s.",
                  getReason()));
        }
      }
      ((DefaultActionInfo) actionInfo).setResult(GsonUtils.GSON.toJson(result, Result.class));
    }
    return ((DefaultActionInfo) actionInfo).getResult();
  }

  @Override
  public Object call() throws Exception {
    List<McResourceInfo> sync = new ArrayList<>();
    if (config.containsKey(JobConfiguration.SOURCE_OBJECT_NAME)
            && !config.get(JobConfiguration.SOURCE_OBJECT_NAME).isEmpty()) {
      result.getAll().add(config.get(JobConfiguration.SOURCE_OBJECT_NAME));
      Resource specResource = OdpsUtils.getResource(
              srcOdps,
              config.get(JobConfiguration.SOURCE_CATALOG_NAME),
              config.get(JobConfiguration.SOURCE_OBJECT_NAME));
      if (StringUtils.isNullOrEmpty(specResource.getName())
              || specResource.getType().equals(Resource.Type.UNKOWN)) {
        result.getInvalid().add(specResource.getName());
        result.setReason("Resource name is EMPTY or type is UNKNOWN.");
        LOG.error("Invalid resource name or type {} for task {}.", specResource.getName(), id);
        throw new MmaException("ERROR: Resource name is empty.");
      }
      sync.add(new McResourceInfo(specResource));
    } else {
      List<String> resourceNames = new ArrayList<>();
      for (Resource resource : destOdps.resources()) {
        resourceNames.add(resource.getName());
      }

      for (Resource resource : srcOdps.resources()) {
        result.getAll().add(resource.getName());
        if (StringUtils.isNullOrEmpty(resource.getName())
                || resource.getType().equals(Resource.Type.UNKOWN)) {
          result.getInvalid().add(resource.getName());
          continue;
        }

        if (resourceNames.contains(resource.getName())) {
          Resource tmp = destOdps.resources().get(resource.getName());
          String contentMD5 = ((FileResource) tmp).getContentMd5();
          // TABLE.TYPE doesn't have contentMd5 which will be updated everytime.
          if (!resource.getType().equals(Resource.Type.TABLE)
                  && ((FileResource) resource).getContentMd5().equals(contentMD5)) {
            result.getExist().add(resource.getName());
            continue;
          }
        }
        sync.add(new McResourceInfo(resource));
      }

      if (result.existAll()) {
        result.setReason("All resources has already exist.");
        LOG.info("All resources has already exist {} for task {}", result.getExist(), id);
        return GsonUtils.GSON.toJson(result, Result.class);
      }

      if (result.getExist().size() != 0) {
        LOG.info("These resources has already exist which don't need to be update {} for task {}.", result.getExist(), id);
      }

      if (result.invalidAll()) {
        result.setReason("All resources are invalid (resource.name == null || resource.type = unknown).");
        LOG.warn("All resources are invalid {} for task {}", result.getInvalid(), id);
        return GsonUtils.GSON.toJson(result, Result.class);
      }

      if (result.getInvalid().size() != 0) {
        LOG.warn("Invalid resources lists {} for task {}.", result.getInvalid(), id);
      }

      if (sync.size() == 0) {
        result.setReason("All resources has already exist or which are invalid.");
        LOG.error("All resources has already exist or which are invalid for task {}.", id);
        throw new MmaException("Error: All resources has already exist or which are invalid.");
      }
    }

    for (McResourceInfo resourceInfo : sync) {
      if (Resource.Type.TABLE.equals(resourceInfo.getType())) {
        OdpsUtils.addTableResource(destOdps, destProject, (TableResource) resourceInfo.toResource(), true);
      } else {
        addFileResource(srcOdps, destOdps, resourceInfo);
      }
      result.getSuccess().add(resourceInfo.getAlias());
    }

    return GsonUtils.GSON.toJson(result, Result.class);
  }

  /**
   * This method doesn't need isUpdate parameter like addTableResource method,
   * and the files [FILE|JAR|PY|ARCHIVE] that are not updated will be processed by the blacklist
   */
  private void addFileResource(
          Odps srcOdps,
          Odps destOdps,
          McResourceInfo resourceInfo) throws OdpsException, MmaException {
    boolean exists = destOdps.resources().exists(resourceInfo.getAlias());
    if (exists) {
      LOG.info("Update resource {} because of exists for task {}.", resourceInfo.getAlias(), id);
      destOdps.resources().update((FileResource) resourceInfo.toResource(), srcOdps.resources().getResourceAsStream(resourceInfo.getAlias()));
    } else {
      LOG.info("Create new resource {} for task {}.", resourceInfo.getAlias(), id);
      destOdps.resources().create((FileResource) resourceInfo.toResource(), srcOdps.resources().getResourceAsStream(resourceInfo.getAlias()));
    }
  }

  @Setter
  @Getter
  private static class Result {
    private List<String> all = new ArrayList<>();
    private List<String> success = new ArrayList<>();
    private List<String> exist = new ArrayList<>();
    private List<String> invalid = new ArrayList<>();
    private List<String> failed = new ArrayList<>();
    private String reason;

    private boolean existAll() {
      return exist.size() == all.size();
    }

    private boolean invalidAll() {
      return invalid.size() == all.size();
    }

    private boolean successExistInvalidAll() {
      return (success.size() + exist.size() + invalid.size()) == all.size();
    }

    @Override
    public String toString() {
      return "[all]: " + all.toString()
              + "\n[success]: " + success.toString()
              + "\n[exist]: " + exist.toString()
              + "\n[invalid]: " + invalid.toString()
              + "\n[failed]: " + failed.toString()
              + "\n[reason]: " + reason;
    }
  }
}
