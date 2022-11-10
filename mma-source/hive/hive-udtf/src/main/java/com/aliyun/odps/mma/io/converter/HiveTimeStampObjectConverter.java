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

import com.aliyun.odps.type.TypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;

public class HiveTimeStampObjectConverter extends AbstractHiveObjectConverter {
  Class<?> timestampInspectorClazz;
  Method getPrimitiveJavaObjectMethod;
  Method toEpochMilliMethod;
  Method getNanosMethod;

  public HiveTimeStampObjectConverter() {
    try {
      this.timestampInspectorClazz = Class.forName("org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector");
      this.getPrimitiveJavaObjectMethod = this.timestampInspectorClazz.getDeclaredMethod("getPrimitiveJavaObject", Object.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      Class<?> hiveTimestampClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
      this.toEpochMilliMethod = hiveTimestampClazz.getDeclaredMethod("toEpochMilli");
      this.getNanosMethod = hiveTimestampClazz.getDeclaredMethod("getNanos");
    } catch (Exception e) {
      // ignore
    }

  }

  @Override
  public Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {
    if (o == null) {
      return null;
    }

    Object rawTs;
    try {
      rawTs = this.getPrimitiveJavaObjectMethod.invoke(this.timestampInspectorClazz.cast(objectInspector), o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    switch (rawTs.getClass().getName()) {
      case "org.apache.hadoop.hive.common.type.Timestamp":
        try {
          Timestamp jt = new Timestamp((Long) this.toEpochMilliMethod.invoke(rawTs));
          jt.setNanos((Integer) this.getNanosMethod.invoke(rawTs));
          return jt;
        } catch (IllegalAccessException | InvocationTargetException var8) {
          throw new RuntimeException(var8);
        }
      case "java.sql.Timestamp":
        return rawTs;
      default:
        throw new RuntimeException(String.format("unreachable!, get timestamp class %s", rawTs.getClass().getName()));
    }
  }
}
