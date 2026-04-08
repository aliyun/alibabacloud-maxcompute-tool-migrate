package com.aliyun.odps.mma.meta.schema.type;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;

public class ExtendedTimeType implements TypeInfo {

  @Override
  public String getTypeName() {
    return "TIME";
  }

  @Override
  public OdpsType getOdpsType() {
    return null;
  }
}
