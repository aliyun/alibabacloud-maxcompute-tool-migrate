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
import java.util.stream.Collectors;

public class SubstGroup implements Subst {

  private final List<Subst> substList;

  public SubstGroup(List<Subst> substList) {
    this.substList = substList.stream()
                              .sorted(Comparator.comparing(Subst::start).reversed()).collect(Collectors.toList());
  }

  public List<Subst> getSubstList() {
    return substList;
  }

  @Override
  public int start() {
    return substList.stream().map(Subst::start).min(Comparator.comparing(a->a)).orElse(0);
  }

  @Override
  public int end() {
    return substList.stream().map(Subst::end).max(Comparator.comparing(a->a)).orElse(0);
  }

  @Override
  public String subst(String sql) {
    for (Subst subst : substList) {
      sql = subst.subst(sql);
    }
    return sql;
  }
}
