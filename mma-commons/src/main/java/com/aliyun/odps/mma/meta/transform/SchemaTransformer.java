package com.aliyun.odps.mma.meta.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.Validate;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;

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
