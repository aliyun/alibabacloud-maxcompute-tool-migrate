package com.aliyun.odps.mma.meta.transform;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource.ColumnMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel.TableMetaModelBuilder;

public class HiveSchemaTransformer implements SchemaTransformer {

  private TypeTransformer typeTransformer = new HiveTypeTransformer();

  @Override
  public SchemaTransformResult transform(TableMetaModel source, JobConfiguration config) {
    List<TypeTransformResult> typeTransformResults = new ArrayList<>();
    List<ColumnMetaModel> mcColumnMetaModels = new ArrayList<>(source.getColumns().size());

    for (ColumnMetaModel hiveColumnMetaModel : source.getColumns()) {
      // TODO: to MC type v1
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
        config.getOrDefault(JobConfiguration.DEST_CATALOG_NAME, source.getDatabase()),
        config.getOrDefault(JobConfiguration.DEST_OBJECT_NAME, source.getTable()),
        mcColumnMetaModels);

    if (!source.getPartitionColumns().isEmpty()) {
      List<ColumnMetaModel> mcPartitionColumnMetaModels =
          new ArrayList<>(source.getPartitionColumns().size());
      for (ColumnMetaModel hivePartitionColumnMetaModel : source.getPartitionColumns()) {
        if ("DATE".equalsIgnoreCase(hivePartitionColumnMetaModel.getType())) {
          mcPartitionColumnMetaModels.add(
              new ColumnMetaModel(
                  hivePartitionColumnMetaModel.getColumnName(),
                  "STRING",
                  hivePartitionColumnMetaModel.getComment()));
        }
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
