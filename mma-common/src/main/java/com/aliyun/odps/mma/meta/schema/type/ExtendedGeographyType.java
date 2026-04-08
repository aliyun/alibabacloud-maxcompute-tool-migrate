package com.aliyun.odps.mma.meta.schema.type;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;

public class ExtendedGeographyType implements TypeInfo {

  @Override
  public String getTypeName() {
    return "GEOGRAPHY";
  }

  @Override
  public OdpsType getOdpsType() {
    return null;
  }
}
