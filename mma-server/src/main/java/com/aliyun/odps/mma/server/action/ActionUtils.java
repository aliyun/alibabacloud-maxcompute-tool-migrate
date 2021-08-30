package com.aliyun.odps.mma.server.action;

import java.util.List;

public class ActionUtils {

  public static void toVerificationResult(List<List<Object>> result) {
    for (List<Object> row : result) {
      for (int i = 0; i < row.size() - 1; i++) {
        // keep last count type == long
        row.set(i, row.get(i).toString());
      }
    }
  }

}
