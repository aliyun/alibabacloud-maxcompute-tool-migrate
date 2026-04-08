package com.aliyun.odps.mma.meta.schema.type;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;

public class ExtendedRangeType implements TypeInfo {

  @Override
  public String getTypeName() {
    return "RANGE";
  }

  @Override
  public OdpsType getOdpsType() {
    return null;
  }
}
