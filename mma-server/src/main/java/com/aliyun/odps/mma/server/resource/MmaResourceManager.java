/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mma.server.resource;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MmaResourceManager {

  private static final Logger LOG = LogManager.getLogger(MmaResourceManager.class);

  private static MmaResourceManager instance;

  private Map<Resource, Long> resourceMap = new HashMap<>();

  private MmaResourceManager() {}

  /**
   * Allocate resource.
   *
   * @param actionId Action ID.
   * @param resources Resources requirements.
   * @return Allocated resources, may be different from resource requirements, or null if the
   * requirements cannot be satisfied.
   */
  public synchronized Map<Resource, Long> allocate(
      String actionId,
      Map<Resource, Long> resources) {

    LOG.info("Allocate resource for {}, required: {}, current: {}",
             actionId, resources, resourceMap);

    for (Resource resource : resources.keySet()) {
      Long requiredNum = resources.get(resource);
      Long availableNum = resourceMap.get(resource);
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
      Long requiredNum = resources.get(resource);
      Long availableNum = resourceMap.get(resource);
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
  public synchronized void release(String actionId, Map<Resource, Long> resources) {
    // Update the resource map
    LOG.info("Release resource from {}, resources: {}", actionId, resources);
    for (Resource resource : resources.keySet()) {
      Long releasedNum = resources.get(resource);
      Long availableNum = resourceMap.get(resource);
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
  public void update(Resource resource, Long number) {
    if (resourceMap.containsKey(resource)) {
      LOG.warn("Found duplicated resource config, will overwrite");
    }
    LOG.info("Update resource map, key: {}, before: {}, after: {}",
             resource, resourceMap.get(resource), number);
    resourceMap.put(resource, number);
  }

  public synchronized static MmaResourceManager getInstance() {
    if (instance == null) {
      instance = new MmaResourceManager();
    }

    return instance;
  }
}
