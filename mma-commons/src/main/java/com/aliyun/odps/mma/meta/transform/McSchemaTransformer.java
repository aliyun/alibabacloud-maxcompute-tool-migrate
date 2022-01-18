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
import java.util.List;

import com.aliyun.odps.mma.config.DataDestType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.model.ColumnMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel;
import com.aliyun.odps.mma.meta.model.TableMetaModel.TableMetaModelBuilder;

public class McSchemaTransformer implements SchemaTransformer {

  @Override
  public SchemaTransformResult transform(TableMetaModel source, JobConfiguration config) {
    DataDestType dataDestType = DataDestType.valueOf(config.get(JobConfiguration.DATA_DEST_TYPE));
    TypeTransformer typeTransformer;
    if (!DataDestType.MaxCompute.equals(dataDestType)) {
      typeTransformer = new McTypeTransformer(true);
    } else {
      typeTransformer = new McTypeTransformer(false);
    }

    List<TypeTransformResult> typeTransformResults = new ArrayList<>();
    List<ColumnMetaModel> mcColumnMetaModels = new ArrayList<>(source.getColumns().size());

    for (ColumnMetaModel hiveColumnMetaModel : source.getColumns()) {
      TypeTransformResult typeTransformResult = typeTransformer.toMcTypeV2(
          hiveColumnMetaModel.getColumnName(), hiveColumnMetaModel.getType());
      typeTransformResults.add(typeTransformResult);
      mcColumnMetaModels.add(
          new ColumnMetaModel(
              hiveColumnMetaModel.getColumnName(),
              typeTransformResult.getTransformedType(),
              hiveColumnMetaModel.getComment()));
    }

    TableMetaModelBuilder builder = new TableMetaModelBuilder(
        config.get(JobConfiguration.DEST_CATALOG_NAME),
        config.get(JobConfiguration.DEST_OBJECT_NAME),
        mcColumnMetaModels);

    if (!source.getPartitionColumns().isEmpty()) {
      List<ColumnMetaModel> mcPartitionColumnMetaModels =
          new ArrayList<>(source.getPartitionColumns().size());
      for (ColumnMetaModel hivePartitionColumnMetaModel : source.getPartitionColumns()) {
        TypeTransformResult typeTransformResult = typeTransformer.toMcTypeV2(
            hivePartitionColumnMetaModel.getColumnName(),
            hivePartitionColumnMetaModel.getType());
        typeTransformResults.add(typeTransformResult);
        mcPartitionColumnMetaModels.add(
            new ColumnMetaModel(
                hivePartitionColumnMetaModel.getColumnName(),
                typeTransformResult.getTransformedType(),
                hivePartitionColumnMetaModel.getComment()));
      }
      builder.partitionColumns(mcPartitionColumnMetaModels);
      builder.partitions(source.getPartitions());
    }
    return new SchemaTransformResult(builder.build(), typeTransformResults);
  }
}
