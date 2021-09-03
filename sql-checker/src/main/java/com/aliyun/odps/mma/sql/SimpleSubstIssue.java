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

import java.util.List;

public class SimpleSubstIssue implements SubstIssue {

  private final Issue issue;
  private final Subst subst;

  SimpleSubstIssue(Issue issue, Subst subst) {
    this.issue = issue;
    this.subst = subst;
  }

  public SimpleSubstIssue(Issue issue, List<Subst> subst) {
    this.issue = issue;
    if (subst.size() == 1) {
      this.subst = subst.get(0);
    } else {
      this.subst = new SubstGroup(subst);
    }
  }

  @Override
  public int start() {
    return subst.start();
  }

  @Override
  public int end() {
    return subst.end();
  }

  @Override
  public String subst(String sql) {
    return subst.subst(sql);
  }

  @Override
  public CompatibilityLevel getCompatibility() {
    return issue.getCompatibility();
  }

  @Override
  public String getDescription() {
    return issue.getDescription();
  }

  @Override
  public String getSuggestion() {
    return issue.getSuggestion();
  }
}
