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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.mma.meta.transform.TypeTransformIssue;
import com.aliyun.odps.mma.meta.transform.TypeTransformResult;
import com.aliyun.odps.mma.meta.transform.TypeTransformer;

public class HiveTypeTransformer implements TypeTransformer {

  private static final String DECIMAL_INCOMPATIBILITY_REASON =
      "The number of digits of 'DECIMAL' type in ODPS is different from HIVE";
  private static final String TIMESTAMP_INCOMPATIBILITY_REASON =
      "ODPS supports microseconds precision, while HIVE supports nanosecond precision";
  private static final String STRING_INCOMPATIBILITY_REASON =
      "String in ODPS cannot exceed 8MB";

  /**
   * Numeric types
   */
  private static final String TINYINT = "TINYINT";
  private static final String SMALLINT = "SMALLINT";
  private static final String INT = "INT";
  private static final String BIGINT = "BIGINT";
  private static final String FLOAT = "FLOAT";
  private static final String DOUBLE = "DOUBLE";
  private static final String DECIMAL = "DECIMAL(\\([\\d]+,\\s*[\\d]+\\))?";

  /**
   * String types
   */
  private static final String STRING = "STRING";
  private static final String VARCHAR = "VARCHAR(\\([\\d]+\\))";
  private static final String CHAR = "CHAR(\\([\\d]+\\))";

  /**
   * Date and time types
   */
  private static final String TIMESTAMP = "TIMESTAMP";
  private static final String DATE = "DATE";

  /**
   * Misc types
   */
  private static final String BOOLEAN = "BOOLEAN";
  private static final String BINARY = "BINARY";

  /**
   * Complex types
   */
  private static final String ARRAY = "ARRAY<(.+)>";
  private static final String MAP = "MAP<(.+)>";
  private static final String STRUCT = "STRUCT<(.+)>";


  @Override
  public TypeTransformResult toMcTypeV1(String columnName, String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeTransformResult toMcTypeV2(String columnName, String hiveType) {
    hiveType = hiveType.toUpperCase().trim();

    String transformedType = null;
    TypeTransformIssue typeTransformIssue = TypeTransformIssue.OK();
    if (hiveType.matches(TINYINT)) {
      transformedType = "TINYINT";
    } else if (hiveType.matches(SMALLINT)) {
      transformedType = "SMALLINT";
    } else if (hiveType.matches(INT)) {
      transformedType = "INT";
    } else if (hiveType.matches(BIGINT)) {
      transformedType = "BIGINT";
    } else if (hiveType.matches(FLOAT)) {
      transformedType = "FLOAT";
    } else if (hiveType.matches(DOUBLE)) {
      transformedType = "DOUBLE";
    } else if (hiveType.matches(DECIMAL)) {
      transformedType = "DECIMAL(36,18)";
      typeTransformIssue = TypeTransformIssue.getInCompatibleTypeRisk(hiveType, transformedType,
                                                                      DECIMAL_INCOMPATIBILITY_REASON);
    } else if (hiveType.matches(TIMESTAMP)) {
      transformedType = "TIMESTAMP";
      typeTransformIssue = TypeTransformIssue.getInCompatibleTypeRisk(hiveType, transformedType,
                                                                      TIMESTAMP_INCOMPATIBILITY_REASON);
    } else if (hiveType.matches(DATE)) {
      transformedType = "DATETIME";
    } else if (hiveType.matches(STRING)) {
      transformedType = "STRING";
      typeTransformIssue = TypeTransformIssue
          .getInCompatibleTypeRisk(hiveType, transformedType, STRING_INCOMPATIBILITY_REASON);
    } else if (hiveType.matches(VARCHAR)) {
      Pattern pattern = Pattern.compile(VARCHAR);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      transformedType = "VARCHAR" + matcher.group(1);
    } else if (hiveType.matches(CHAR)) {
      transformedType = "STRING";
    } else if (hiveType.matches(BOOLEAN)) {
      transformedType = "BOOLEAN";
    } else if (hiveType.matches(BINARY)) {
      transformedType = "BINARY";
    } else if (hiveType.matches(ARRAY)) {
      Pattern pattern = Pattern.compile(ARRAY);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();

      TypeTransformResult elementTypeTransformResult =
          toMcTypeV2(columnName, matcher.group(1).trim());
      transformedType = "ARRAY" + "<" + elementTypeTransformResult.getTransformedType() + ">";
    } else if (hiveType.matches(MAP)) {
      Pattern pattern = Pattern.compile(MAP);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      // The type of key in a map must be a primitive type, so there is no comma in its type
      // definition. So we can split the type tuple of key and value by the first comma.
      String typeTuple = matcher.group(1);
      int firstCommaIdx = typeTuple.indexOf(',');
      String keyType = typeTuple.substring(0, firstCommaIdx).trim();
      String valueType = typeTuple.substring(firstCommaIdx + 1).trim();
      TypeTransformResult keyTypeTransformResult = toMcTypeV2(columnName, keyType);
      TypeTransformResult valueTypeTransformResult = toMcTypeV2(columnName, valueType);
      transformedType = "MAP<" + keyTypeTransformResult.getTransformedType() + "," +
          valueTypeTransformResult.getTransformedType() + ">";
    } else if (hiveType.matches(STRUCT)) {
      Pattern pattern = Pattern.compile(STRUCT);
      Matcher matcher = pattern.matcher(hiveType);
      matcher.matches();
      // Since the type definition of a struct can be very complex and may contain any possible
      // character in a type definition, we have to split the type list properly so that we can
      List<String> fieldDefinitions = splitStructFields(matcher.group(1));

      List<String> odpsFieldDefinitions = new ArrayList<>();
      for (String fieldDefinition : fieldDefinitions) {
        // Remove comments, not supported
        int commentIdx = fieldDefinition.toUpperCase().indexOf("COMMENT");
        if (commentIdx != -1) {
          fieldDefinition = fieldDefinition.substring(0, commentIdx);
        }

        // The type of a struct field can be another struct, which may contain colons. So we have
        // to split the field definition by the first colon.
        int firstColonIdx = fieldDefinition.indexOf(':');
        String fieldName = fieldDefinition.substring(0, firstColonIdx).trim();
        String fieldType = fieldDefinition.substring(firstColonIdx + 1).trim();
        TypeTransformResult fieldTypeTransformResult = toMcTypeV2(columnName, fieldType);
        odpsFieldDefinitions.add(
            fieldName + ":" + fieldTypeTransformResult.getTransformedType());
      }
      transformedType = "STRUCT<" + String.join(",", odpsFieldDefinitions) + ">";
    } else {
      typeTransformIssue = TypeTransformIssue.getUnsupportedTypeRisk(hiveType);
    }

    return new TypeTransformResult(columnName, hiveType, transformedType, typeTransformIssue);
  }

  private List<String> splitStructFields(String fieldDefinitionString) {
    int bracketsCounter = 0;
    boolean split = true;
    int startIdx = 0;
    List<String> fieldDefinitions = new ArrayList<>();

    for (int i = 0; i < fieldDefinitionString.length(); i++) {
      char c = fieldDefinitionString.charAt(i);
      if (c == '<' || c == '(') {
        split = false;
        bracketsCounter += 1;
      } else if (c == '>' || c == ')') {
        bracketsCounter -= 1;
        if (bracketsCounter == 0) {
          split = true;
        }
      } else if (c == ',') {
        if (split) {
          fieldDefinitions.add(fieldDefinitionString.substring(startIdx, i).trim());
          startIdx = i + 1;
        }
      }
    }
    fieldDefinitions.add(fieldDefinitionString.substring(startIdx).trim());

    return fieldDefinitions;
  }
}

