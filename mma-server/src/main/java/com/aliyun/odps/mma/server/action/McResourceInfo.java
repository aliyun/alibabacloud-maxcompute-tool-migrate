/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.server.action;

import com.aliyun.odps.ArchiveResource;
import com.aliyun.odps.FileResource;
import com.aliyun.odps.JarResource;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.PyResource;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;
import com.aliyun.odps.mma.exception.MmaException;
import com.aliyun.odps.utils.StringUtils;

public class McResourceInfo {

  private String alias;
  private Resource.Type type;
  private String comment;
  private String tableName;
  private String partitionSpec;

  public McResourceInfo(String alias, Resource.Type type, String comment, String tableName, String partitionSpec) {
    this.alias = alias;
    this.type = type;
    this.comment = comment;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
  }

  public McResourceInfo(Resource resource) {
    String tableName = null;
    String partitionSpec = null;
    if (Resource.Type.TABLE.equals(resource.getType())) {
      TableResource tableResource = (TableResource) resource;
      tableName = tableResource.getSourceTable().getName();
      PartitionSpec spec = tableResource.getSourceTablePartition();
      if (spec != null) {
        partitionSpec = spec.toString();
      }
    }

    this.alias = resource.getName();
    this.type = resource.getType();
    this.comment = resource.getComment();
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
  }

  public Resource toResource() throws MmaException {
    Resource resource;
    switch (type) {
      case ARCHIVE:
        resource = new ArchiveResource();
        break;
      case PY:
        resource = new PyResource();
        break;
      case JAR:
        resource = new JarResource();
        break;
      case FILE:
        resource = new FileResource();
        break;
      case TABLE:
        PartitionSpec spec = null;
        if (!StringUtils.isNullOrEmpty(partitionSpec)) {
          spec = new PartitionSpec(partitionSpec);
        }
        resource = new TableResource(getTableName(), null, spec);
        break;
      default:
        throw new MmaException("Unknown resource type: " + getType());
    }

    resource.setName(alias);
    resource.setComment(comment);
    return resource;
  }

  public Resource.Type getType() {
    return type;
  }

  public String getAlias() {
    return alias;
  }

  public String getComment() {
    return comment;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionSpec() {
    return partitionSpec;
  }

}
