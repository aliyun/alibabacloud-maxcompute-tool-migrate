package com.aliyun.odps.datacarrier.taskscheduler;

import com.aliyun.odps.OdpsType;

public class OdpsTypeTransformer implements TypeTransformer {
  private boolean transferToExternalTable;

  public OdpsTypeTransformer(boolean transferToExternalTable) {
    this.transferToExternalTable = transferToExternalTable;
  }

  @Override
  public TypeTransformResult toOdpsTypeV1(String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeTransformResult toOdpsTypeV2(String type) {
    String transformedType = type.toUpperCase();
    if (this.transferToExternalTable && OdpsType.DATETIME.name().equals(transformedType)) {
      transformedType = OdpsType.TIMESTAMP.name();
    }
    return new TypeTransformResult(DataSource.ODPS,
        type,
        transformedType,
        new Risk(Risk.RISK_LEVEL.LOW, ""));
  }
}
