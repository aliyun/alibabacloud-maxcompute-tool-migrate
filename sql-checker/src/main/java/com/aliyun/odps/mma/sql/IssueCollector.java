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
