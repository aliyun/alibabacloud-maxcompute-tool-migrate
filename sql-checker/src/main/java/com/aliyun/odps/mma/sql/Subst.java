package com.aliyun.odps.mma.sql;

public interface Subst {
  /**
   * @return starting char index in input sql
   */
  int start();

  /**
   * @return ending char index in input sql
   */
  int end();

  /**
   * replace input sql into a correct one according to issue suggestions
   * @param sql input sql
   * @return corrected sql
   */
  String subst(String sql);
}
