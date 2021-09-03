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

import com.aliyun.odps.compiler.ConsoleLogger;
import com.aliyun.odps.compiler.SubstScanner;
import com.aliyun.odps.compiler.SyntaxTreeNode;
import com.aliyun.odps.compiler.log.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;

public class LogItemIssue implements Issue {

  private static ResourceBundle suggestions = ResourceBundle.getBundle("suggestions");
  private final LogItem logItem;

  private LogItemIssue(LogItem logItem) {
    this.logItem = logItem;
  }

  @Override
  public CompatibilityLevel getCompatibility() {
    if (suggestions.containsKey(logItem.getMessageKey() + ".lv")) {
      String level = suggestions.getString(logItem.getMessageKey() + ".lv");
      return CompatibilityLevel.values()[Integer.parseInt(level)];
    }
    return logItem.getLevel() == LogLevel.WARNING ? CompatibilityLevel.WEEK_WARNINGS : CompatibilityLevel.ERROR;
  }

  @Override
  public String getDescription() {
    int line = 0;
    int col = 0;
    if (logItem.getPos() != null) {
      line = logItem.getPos().getLine();
      col = logItem.getPos().getCol();
    }
    return String.format(
        "[line %s, col %s] %s",
        line,
        col,
        StringUtils.defaultString(CompilerMessages.formatLogItemMessage(logItem), "N/A"));
  }

  @Override
  public String getSuggestion() {
    if (suggestions.containsKey(logItem.getMessageKey())) {
      return suggestions.getString(logItem.getMessageKey());
    }
    return null;
  }

  private Issue subst(SyntaxTreeNode ast) {
    if (suggestions.containsKey(logItem.getMessageKey() + ".subst")) {
      String level = suggestions.getString(logItem.getMessageKey() + ".subst");
      try {
        Subst subst = ((SubstScanner) Class.forName(level).newInstance()).subst(logItem, ast);
        if (subst != null) {
          return new SimpleSubstIssue(this, subst);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return null;
  }

  public static class IssueLogger extends DecoratorLogger {

    private final List<Issue> issues = new ArrayList<>();

    IssueLogger() {
      super(new ConsoleLogger());
    }

    List<Issue> getIssues(SyntaxTreeNode ast) {
      for (int i = 0; i < issues.size(); i++) {
        Issue issue = issues.get(i);
        if (issue instanceof LogItemIssue) {
          Issue subst = ((LogItemIssue) issue).subst(ast);
          if (subst != null) {
            issues.set(i, subst);
          }
        }
      }
      return issues;
    }

    @Override
    public void log(LogItem logItem) {
      if (logItem.getLevel().getSeverity() <= LogLevel.WARNING.getSeverity()) {
        LogItemIssue e = new LogItemIssue(logItem);
        issues.add(e);
      }
    }

    @Override
    public void log(LogItemGroup logItemGroup) {
      for (LogItem item : logItemGroup.getItems()) {
        log(item);
      }
    }
  }
}
