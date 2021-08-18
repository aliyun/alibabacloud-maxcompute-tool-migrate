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
