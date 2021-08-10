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

package com.aliyun.odps.mma.server;


import org.apache.commons.lang3.Validate;

public class TypeTransformResult {

  private String columnName;
  private String originalType;
  private String transformedType;
  private TypeTransformIssue typeTransformIssue;

  public TypeTransformResult(
      String columnName,
      String originalType,
      String transformedType,
      TypeTransformIssue risk) {
    this.columnName = Validate.notNull(columnName);
    this.originalType = Validate.notNull(originalType);
    this.transformedType = Validate.notNull(transformedType);
    this.typeTransformIssue = Validate.notNull(risk);
  }

  public String getColumnName() {
    return columnName;
  }

  public String getOriginalType() {
    return this.originalType;
  }

  public String getTransformedType() {
    return this.transformedType;
  }

  public TypeTransformIssue getTypeTransformIssue() {
    return this.typeTransformIssue;
  }
}
