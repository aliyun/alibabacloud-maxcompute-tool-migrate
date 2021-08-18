package com.aliyun.odps.mma.sql;

public interface Issue {

  /**
   * @return the CompatibilityLevel (except OK)
   */
  CompatibilityLevel getCompatibility();

  /**
   * @return a string description for the incompatible issue
   */
  String getDescription();

  /**
   * @return sql modification suggestions
   */
  String getSuggestion();
}
