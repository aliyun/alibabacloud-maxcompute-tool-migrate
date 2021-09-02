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

import com.aliyun.odps.mma.sql.utils.PrintUtils;

/***
 * Author: jinpeng.wjp
 * Date: 2019/4/19
 * Compatibility level. Used as a conclusion
 */
public enum CompatibilityLevel {
  /**
   * Fully compatible
   */
  OK {
    @Override
    public String toString() {
      return PrintUtils.toGreenString(this.name());
    }
  },

  /**
   * SQL is available in ODPS. And there are some week warnings
   */
  WEEK_WARNINGS {
    @Override
    public String toString() {
      return PrintUtils.toYellowString(this.name());
    }
  },

  /**
   * SQL is available in ODPS.
   * However, there is some known issues that may cause some problems.
   * For example, the result may be incompatible in certain input data
   */
  STRONG_WARNINGS {
    @Override
    public String toString() {
      return PrintUtils.toYellowString(this.name());
    }
  },

  /**
   * SQL is not available in ODPS and must be fixed
   */
  ERROR {
    @Override
    public String toString() {
      return PrintUtils.toRedString(this.name());
    }
  };

  @Override
  public abstract String toString();
}
