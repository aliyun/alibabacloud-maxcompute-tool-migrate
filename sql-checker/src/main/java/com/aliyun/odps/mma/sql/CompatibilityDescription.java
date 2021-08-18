package com.aliyun.odps.mma.sql;

import java.util.Comparator;
import java.util.List;

/**
 * Result of compatibility checking.
 * Including a CompatibilityLevel as a simple conclusions together with other details
 */
public interface CompatibilityDescription {

  /**
   * @return CompatibilityLevel as simple conclusion
   */
  default CompatibilityLevel getCompatibility() {
    return getIssues().stream().map(Issue::getCompatibility)
                      .max(Comparator.comparing(Enum::ordinal)).orElse(CompatibilityLevel.OK);
  }

  /**
   * @return all discovered issues. No ordering guarantees
   */
  List<Issue> getIssues();

  /**
   * auto transform user sql to suggestion one according to suggestions
   * Note that this modification is not always complete
   * @return auto transformed sql
   */
  String transform();
}
