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

package com.aliyun.odps.mma.server.action;

import java.util.ArrayList;
import java.util.List;

public class ActionUtils {

  public static List<List<Object>> toVerificationResult(List<List<Object>> result) {
    // Verification Result:
    // [
    //  [pt1 value, pt2 value, ... ,count],
    //  [pt1 value, pt2 value, ... ,count],
    //  ...
    // ]
    // Convert all pt value to String type
    // Keep count type Long
    List<List<Object>> verificationResult = new ArrayList<>(result.size());
    for (List<Object> row : result) {
      List<Object> newRow = new ArrayList<>(row.size());
      int i = 0;
      while (i < row.size() - 1) {
        newRow.add(row.get(i).toString());
        i += 1;
      }
      // keep last count type == long
      newRow.add(row.get(i));
      verificationResult.add(newRow);
    }
    return verificationResult;
  }

}
