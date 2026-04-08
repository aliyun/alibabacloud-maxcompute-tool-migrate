package com.aliyun.odps.mma.meta.schema.type;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;

public class ExtendedIntervalType implements TypeInfo {

  @Override
  public String getTypeName() {
    return "INTERVAL";
  }

  @Override
  public OdpsType getOdpsType() {
    return null;
  }
}
