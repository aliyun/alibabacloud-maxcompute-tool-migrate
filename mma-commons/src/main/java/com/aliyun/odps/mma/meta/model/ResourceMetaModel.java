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
 *
 */

package com.aliyun.odps.mma.meta.model;

import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Resource;
import com.aliyun.odps.TableResource;

public class ResourceMetaModel {

  private String alias;
  private Resource.Type type;
  private String comment;
  private String tableName;
  private String partitionSpec;

  public ResourceMetaModel(String alias, Resource.Type type, String comment, String tableName, String partitionSpec) {
    this.alias = alias;
    this.type = type;
    this.comment = comment;
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
  }

  public ResourceMetaModel(Resource resource) {
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
