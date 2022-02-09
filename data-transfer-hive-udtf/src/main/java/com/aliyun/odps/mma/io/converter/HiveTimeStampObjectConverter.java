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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;

import com.aliyun.odps.type.TypeInfo;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hive.common.util.HiveVersionInfo;

public class HiveTimeStampObjectConverter extends AbstractHiveObjectConverter {

  @Override
  public Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {
    if (o == null) {
      return null;
    }

    try {
      Class<?> timestampInspectorClazz = Class.forName(
          "org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector");
      Method getPrimitiveJavaObject = timestampInspectorClazz
          .getDeclaredMethod("getPrimitiveJavaObject", Object.class);
      Object timestamp = getPrimitiveJavaObject.invoke(timestampInspectorClazz.cast(objectInspector), o);
      if (HiveVersionInfo.getVersion().startsWith("3")) {
        Class<?> hiveTimestampClazz = Class.forName("org.apache.hadoop.hive.common.type.Timestamp");
        Method toEpochMilli = hiveTimestampClazz.getDeclaredMethod("toEpochMilli");
        Method getNanos = hiveTimestampClazz.getDeclaredMethod("getNanos");
        Timestamp t = new Timestamp((Long) toEpochMilli.invoke(timestamp));
        t.setNanos((Integer) getNanos.invoke(timestamp));
        return t;
      } else {
        return timestamp;
      }
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
