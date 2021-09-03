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

package com.aliyun.odps.mma.meta.transform;

import org.apache.commons.lang3.Validate;

public class TypeTransformIssue {

  public enum CompatibilityLevel {
    /**
     * There are risks that must be taken care of manually
     */
    ERROR,
    /**
     * There are risks, but can be solved automatically
     */
    WARNING,
    /**
     * No risk
     */
    OK
  }

  private CompatibilityLevel compatibilityLevel;
  private String description;

  public TypeTransformIssue(CompatibilityLevel compatibilityLevel, String description) {
    this.compatibilityLevel = Validate.notNull(compatibilityLevel);
    this.description = description;
  }

  public CompatibilityLevel getCompatibilityLevel() {
    return compatibilityLevel;
  }

  public String getDescription() {
    return this.description;
  }

  public static TypeTransformIssue getUnsupportedTypeRisk(String originalType) {
    String description = "Unsupported type: " + originalType + ", have to be handled manually";
    return new TypeTransformIssue(CompatibilityLevel.ERROR, description);
  }

  public static TypeTransformIssue getInCompatibleTypeRisk(String originalType, String transformedType,
                                                           String reason) {
    String description = "Incompatible type \'" + originalType + "\', can be transform to \'" +
        transformedType + "\' automatically, but may cause problem. Reason: " + reason;
    return new TypeTransformIssue(CompatibilityLevel.WARNING, description);
  }

  public static TypeTransformIssue getTableNameConflictRisk(String firstDatabaseName, String firstTableName,
                                                            String secondDatabaseName, String secondTableName) {
    String description = "Table name conflict: " + firstDatabaseName + "." + firstTableName +
        " is mapped to the same odps table as " + secondDatabaseName + "." + secondTableName;
    return new TypeTransformIssue(CompatibilityLevel.ERROR, description);
  }

  public static TypeTransformIssue OK() {
    return new TypeTransformIssue(CompatibilityLevel.OK, null);
  }
}
