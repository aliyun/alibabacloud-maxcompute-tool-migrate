package com.aliyun.odps.mma.meta.schema.type;

import java.util.Objects;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;

import lombok.Getter;

@Getter
public class ExtendedBigNumericType implements TypeInfo {

  public ExtendedBigNumericType(Integer precision, Integer scale) {
    this.precision = precision;
    this.scale = scale;
  }

  public ExtendedBigNumericType() {
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
      return "BIGNUMERIC";
    }
    return String.format("%s(%s,%s)", "BIGNUMERIC", precision, scale);
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
    ExtendedBigNumericType that = (ExtendedBigNumericType) o;
    return Objects.equals(precision, that.precision) && Objects.equals(scale, that.scale);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), precision, scale);
  }

  public DecimalTypeInfo toDecimalTypeInfo(int defaultPrecision, int defaultScale) {
    if (precision == null || scale == null) {
      return TypeInfoFactory.getDecimalTypeInfo(defaultPrecision, defaultScale);
    }
    if (precision > 38 || scale > 30) {
//      return TypeInfoFactory.getDecimalTypeInfo(defaultPrecision, defaultScale);
      throw new IllegalArgumentException(
          "BigNumericType precision or scale is too large," + " (" + precision + ", " + scale + ")");
    }
    return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
  }
}
