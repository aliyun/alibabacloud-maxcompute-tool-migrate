package com.aliyun.odps.mma.server;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.mma.config.DataDestType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel.TableMetaModelBuilder;

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
