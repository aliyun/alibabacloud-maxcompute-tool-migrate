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

public class SimpleSubst implements Subst {

  private final int start;
  private final int end;
  private final String subst;

  public SimpleSubst(int start, int end, String subst) {
    this.start = start;
    this.end = end;
    this.subst = subst;
  }

  @Override
  public int start() {
    return start;
  }

  @Override
  public int end() {
    return end;
  }

  @Override
  public String subst(String sql) {
    return sql.substring(0, start) + subst + sql.substring(end);
  }
}
