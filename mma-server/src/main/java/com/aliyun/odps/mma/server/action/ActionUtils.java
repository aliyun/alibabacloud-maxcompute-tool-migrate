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
