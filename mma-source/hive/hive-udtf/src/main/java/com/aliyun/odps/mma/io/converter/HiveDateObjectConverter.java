/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.odps.mma.io.converter;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.TypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;

public class HiveDateObjectConverter extends AbstractHiveObjectConverter {
  Class<?> dateInspectorClazz;
  Method getPrimitiveJavaObjectMethod;
  Method toEpochMilliMethod;

  public HiveDateObjectConverter() {
    try {
      dateInspectorClazz = Class.forName("org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector");
      getPrimitiveJavaObjectMethod = dateInspectorClazz.getDeclaredMethod("getPrimitiveJavaObject", Object.class);
    } catch (ClassNotFoundException | NoSuchMethodException  e) {
      throw new RuntimeException(e);
    }

    try {
      Class<?> hiveDateClazz = Class.forName("org.apache.hadoop.hive.common.type.Date");
      this.toEpochMilliMethod = hiveDateClazz.getDeclaredMethod("toEpochMilli");
    } catch (NoSuchMethodException | ClassNotFoundException e) {
      // ignore
    }
  }

  @Override
  public Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {
    if (o == null) {
      return null;
    }

    Object rawDate;
    try {
      rawDate = this.getPrimitiveJavaObjectMethod.invoke(this.dateInspectorClazz.cast(objectInspector), o);
    } catch (InvocationTargetException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }

    Date value;

    switch (rawDate.getClass().getName()) {
      case "org.apache.hadoop.hive.common.type.Date":
        try {
          long dateTimeValue = (Long)this.toEpochMilliMethod.invoke(rawDate);
          value = new Date(dateTimeValue);
          break;
        } catch (IllegalAccessException | InvocationTargetException var10) {
          throw new RuntimeException(var10);
        }
      case "java.sql.Date":
        value = (Date) rawDate;
        break;
      default:
        throw new RuntimeException(String.format("unreachable!, get date class %s", rawDate.getClass().getName()));
    }

    if (OdpsType.STRING.equals(odpsTypeInfo.getOdpsType())) {
      return value.toString();
    } else if (OdpsType.DATETIME.equals(odpsTypeInfo.getOdpsType())) {
      return new Date(value.getTime());
    } else if (OdpsType.DATE.equals(odpsTypeInfo.getOdpsType())) {
      return new Date(value.getTime());
    } else {
      String msg = String.format("Unsupported implicit type conversion: from %s to %s",
                                 "Hive.Date",
                                 "ODPS." + odpsTypeInfo.getOdpsType());
      throw new IllegalArgumentException(msg);
    }
  }
}
