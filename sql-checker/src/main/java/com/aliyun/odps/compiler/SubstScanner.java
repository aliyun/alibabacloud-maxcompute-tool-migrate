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

package com.aliyun.odps.compiler;

import java.util.ArrayList;
import java.util.List;

import com.aliyun.odps.compiler.log.LogItem;
import com.aliyun.odps.mma.sql.Subst;
import com.aliyun.odps.mma.sql.SubstGroup;


public abstract class SubstScanner extends TreeScanner<Subst, LogItem> {
  public Subst subst(LogItem logItem, SyntaxTreeNode ast) {
    return scan(ast, logItem);
  }

  @Override
  public Subst reduce(Subst r1, Subst r2) {
    if (r1 != null && r2 != null) {
      List<Subst> subst = new ArrayList<>();
      if (r1 instanceof SubstGroup) {
        subst.addAll(((SubstGroup) r1).getSubstList());
      } else {
        subst.add(r1);
      }
      if (r2 instanceof SubstGroup) {
        subst.addAll(((SubstGroup) r2).getSubstList());
      } else {
        subst.add(r2);
      }
      return new SubstGroup(subst);
    }
    return r1 == null ? r2 : r1;

  }
}