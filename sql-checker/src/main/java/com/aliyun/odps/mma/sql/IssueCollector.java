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

import com.aliyun.odps.compiler.CaseDetector;
import com.aliyun.odps.compiler.SyntaxTreeNode;

import java.util.List;
import java.util.ResourceBundle;

public class IssueCollector implements CaseDetector.Collector {
  private static ResourceBundle cases = ResourceBundle.getBundle("cases");

  private final List<Issue> issues;

  IssueCollector(List<Issue> issues) {
    this.issues = issues;
  }

  @Override
  public void collect(String key, SyntaxTreeNode ast) {
    if (!cases.containsKey(key)) {
      return;
    }
    issues.add(new Issue() {
      @Override
      public CompatibilityLevel getCompatibility() {
        return CompatibilityLevel.values()[Integer.parseInt(cases.getString(key))];
      }

      @Override
      public String getDescription() {
        return String.format("[line %s, col %s] %s",
                             ast.parseTreeInfo().pos().getLine(),
                             ast.parseTreeInfo().pos().getCol(),
                             cases.getString(key + ".desc"));
      }

      @Override
      public String getSuggestion() {
        String suggKey = key + ".sugg";
        if (!cases.containsKey(suggKey)) {
          return null;
        }
        return cases.getString(suggKey);
      }
    });
  }
}
