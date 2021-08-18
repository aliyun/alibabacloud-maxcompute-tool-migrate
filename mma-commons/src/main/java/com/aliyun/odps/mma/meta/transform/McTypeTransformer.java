/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mma.meta.transform;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.mma.meta.transform.TypeTransformIssue;
import com.aliyun.odps.mma.meta.transform.TypeTransformResult;
import com.aliyun.odps.mma.meta.transform.TypeTransformer;

public class McTypeTransformer implements TypeTransformer {
  private boolean toExternalTable;

  public McTypeTransformer(boolean toExternalTable) {
    this.toExternalTable = toExternalTable;
  }

  @Override
  public TypeTransformResult toMcTypeV1(String columnName, String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeTransformResult toMcTypeV2(String columnName, String type) {
    String transformedType = type.toUpperCase();
    if (this.toExternalTable && OdpsType.DATETIME.name().equals(transformedType)) {
      transformedType = OdpsType.TIMESTAMP.name();
    }
    return new TypeTransformResult(
        columnName,
        type,
        transformedType,
        TypeTransformIssue.OK());
  }
}
