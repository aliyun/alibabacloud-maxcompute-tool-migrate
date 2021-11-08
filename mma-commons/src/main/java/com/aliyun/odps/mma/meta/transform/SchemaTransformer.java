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

package com.aliyun.odps.mma.meta.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.Validate;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.model.TableMetaModel;

public interface SchemaTransformer {

  class SchemaTransformResult {
    private TableMetaModel tableMetaModel;
    private List<TypeTransformResult> typeTransformResults = Collections.emptyList();

    public SchemaTransformResult(
        TableMetaModel tableMetaModel,
        List<TypeTransformResult> typeTransformResults) {
      this.tableMetaModel = Validate.notNull(tableMetaModel);
      if (typeTransformResults != null) {
        this.typeTransformResults = new ArrayList<>(typeTransformResults);
      }
    }

    public TableMetaModel getTableMetaModel() {
      return tableMetaModel;
    }

    public List<TypeTransformResult> getTypeTransformResults() {
      return new ArrayList<>(typeTransformResults);
    }
  }

  SchemaTransformResult transform(TableMetaModel source, JobConfiguration config);
}
