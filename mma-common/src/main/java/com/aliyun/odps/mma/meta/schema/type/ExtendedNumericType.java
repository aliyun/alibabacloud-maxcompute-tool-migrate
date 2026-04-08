package com.aliyun.odps.mma.meta.schema.type;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import lombok.Getter;

import java.util.Objects;

@Getter
public class ExtendedNumericType implements TypeInfo {

  public ExtendedNumericType(Integer precision, Integer scale) {
    this.precision = precision;
    this.scale = scale;
  }

  public ExtendedNumericType() {
    this(null, null);
  }

  @Override
  public OdpsType getOdpsType() {
    return null;
  }

  private final Integer precision;
  private final Integer scale;

  @Override
  public String getTypeName() {
    if (precision == null) {
      return "NUMERIC";
    }
    return String.format("%s(%s,%s)", "NUMERIC", precision, scale);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ExtendedNumericType that = (ExtendedNumericType) o;
    return Objects.equals(precision, that.precision) && Objects.equals(scale, that.scale);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), precision, scale);
  }

  public DecimalTypeInfo toDecimalTypeInfo(int defaultPrecision, int defaultScale) {
    if (precision == null) {
      return TypeInfoFactory.getDecimalTypeInfo(defaultPrecision, defaultScale);
    }
    return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
  }
}
