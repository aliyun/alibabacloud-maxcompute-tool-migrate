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
