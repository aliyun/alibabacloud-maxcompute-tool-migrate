package com.aliyun.odps.datacarrier.taskscheduler.resource;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceAllocator {

  private static ResourceAllocator instance;

  private static final int DEFAULT_NUM_HIVE_DATA_TRANSFER_JOB_RESOURCE = 5;
  private static final int DEFAULT_NUM_HIVE_DATA_TRANSFER_WORKER_RESOURCE = 25;

  private static final Logger LOG = LogManager.getLogger(ResourceAllocator.class);

  private Map<Resource, Integer> resourceMap = new HashMap<>();

  private ResourceAllocator() {
    // All resources should be initialized with their default value
    this.resourceMap.putIfAbsent(
        Resource.HIVE_DATA_TRANSFER_JOB_RESOURCE, DEFAULT_NUM_HIVE_DATA_TRANSFER_JOB_RESOURCE);
    this.resourceMap.putIfAbsent(
        Resource.HIVE_DATA_TRANSFER_WORKER_RESOURCE,
        DEFAULT_NUM_HIVE_DATA_TRANSFER_WORKER_RESOURCE);
  }

  /**
   * Allocate resource.
   *
   * @param actionId Action ID.
   * @param resources Resources requirements.
   * @return Allocated resources, may be different from resource requirements, or null if the
   * requirements cannot be satisfied.
   */
  public synchronized Map<Resource, Integer> allocate(
      String actionId,
      Map<Resource, Integer> resources) {

    LOG.info("Allocate resource for {}, required: {}, current: {}",
             actionId, resources, resourceMap);

    for (Resource resource : resources.keySet()) {
      Integer requiredNum = resources.get(resource);
      Integer availableNum = resourceMap.get(resource);
      if (availableNum == 0) {
        LOG.info("Allocate resource for {} failed, run out of {}", actionId, resource);
        return null;
      }

      // If number of available resource is less than required, allocate all the available resources
      // to this action.
      if (availableNum < requiredNum) {
        LOG.warn("Allocate resource for {}, not enough {}", actionId, resource);
        resources.put(resource, availableNum);
      }
    }

    // Update the resource map
    for (Resource resource : resources.keySet()) {
      Integer requiredNum = resources.get(resource);
      Integer availableNum = resourceMap.get(resource);
      resourceMap.put(resource, availableNum - requiredNum);
    }

    LOG.info("Allocate resource for {} succeed, allocated: {}", actionId, resources);
    return resources;
  }

  /**
   * Release resource.
   *
   * @param actionId Action ID.
   * @param resources Resources to release.
   */
  public synchronized void release(String actionId, Map<Resource, Integer> resources) {
    // Update the resource map
    LOG.info("Release resource from {}, resources: {}", actionId, resources);
    for (Resource resource : resources.keySet()) {
      Integer releasedNum = resources.get(resource);
      Integer availableNum = resourceMap.get(resource);
      resourceMap.put(resource, availableNum + releasedNum);
    }

    LOG.info("Release resource from {} finished, current: {}", actionId, resourceMap);
  }

  /**
   * Update resource map.
   *
   * @param resource Resource to update.
   * @param number Number of the resource.
   */
  public void update(Resource resource, Integer number) {
    if (resourceMap.containsKey(resource)) {
      LOG.warn("Found duplicated resource config, will overwrite");
    }
    LOG.info("Update resource map, key: {}, before: {}, after: {}",
             resource, resourceMap.get(resource), number);
    resourceMap.put(resource, number);
  }

  public synchronized static ResourceAllocator getInstance() {
    if (instance == null) {
      instance = new ResourceAllocator();
    }

    return instance;
  }
}
