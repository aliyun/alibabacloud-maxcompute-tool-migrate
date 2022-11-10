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

import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class HiveListObjectConverter extends AbstractHiveObjectConverter {

  @Override
  public Object convert(ObjectInspector objectInspector, Object o, TypeInfo odpsTypeInfo) {
    if (o == null) {
      return null;
    }

    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    ObjectInspector elementInspector = listObjectInspector.getListElementObjectInspector();
    TypeInfo elementTypeInfo = ((ArrayTypeInfo) odpsTypeInfo).getElementTypeInfo();
    List list = listObjectInspector.getList(o);
    List<Object> newList = new ArrayList<>();
    for (Object element : list) {
      newList.add(HiveObjectConverter.convert(elementInspector, element, elementTypeInfo));
    }

    return newList;
  }
}
