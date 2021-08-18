package com.aliyun.odps.mma.sql;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SqlPreParser {

  private static Pattern addJar = Pattern.compile("add jar\\b([^;]+);");
  private static Pattern createFunction = Pattern.compile("create (temp|temporary)? function\\b([^;]+);");

  static void preParse(String sql, List<Issue> issue) {
    int start = 0;
    StatementScanner scanner = new StatementScanner(sql);
    while (scanner.next()) {
      String stmt = scanner.sql();

      Matcher addJarMatcher = addJar.matcher(stmt);
      if (addJarMatcher.find()) {
        issue.add(new SimpleSubstIssue(new PreParseIssue(
            "add jar should only invoke once during setting up the project in ODPS",
            "remove this statement"), new SimpleSubst(start, start += stmt.length(), "")));
      }

      Matcher createFunctionMatcher = createFunction.matcher(stmt);
      if (createFunctionMatcher.find()) {
        issue.add(new SimpleSubstIssue(new PreParseIssue(
            "create function should only invoke once during setting up the project in ODPS",
            "remove this statement"),
                                       new SimpleSubst(start, start += stmt.length(), "")));
      }
    }
  }

  private static class PreParseIssue implements Issue {

    private final String desc;
    private final String suggestion;

    PreParseIssue(String desc, String suggestion) {
      this.desc = desc;
      this.suggestion = suggestion;
    }

    @Override
    public CompatibilityLevel getCompatibility() {
      return CompatibilityLevel.WEEK_WARNINGS;
    }

    @Override
    public String getDescription() {
      return desc;
    }

    @Override
    public String getSuggestion() {
      return suggestion;
    }
  }

  private static class StatementScanner {
    private final String sql;
    private StringBuilder sb = new StringBuilder();
    private int start = 0;

    StatementScanner(String sql) {
      this.sql = sql;
    }

    boolean next() {
      if (sql.length() == start) {
        return false;
      }
      sb.setLength(0);

      char c = sql.charAt(start++);
      int state = 0;
      char quote = '\'';
      while (true) {
        switch (state) {
          case 0:
            if (c == '\'' || c == '\"') {
              quote = c;
              state = 1;
              break;
            }
            if (c == ';') {
              sb.append(';');
              return true;
            }
          case 1:
            if (c == quote) {
              state = 0;
              break;
            }
            if (c == '\\') {
              sb.append(c);
              if (sql.length() == start) {
                return true;
              }
              c = sql.charAt(start++);
            }
            break;
        }

        sb.append(c);
        if (sql.length() == start) {
          return true;
        }
        c = sql.charAt(start++);
      }
    }

    public String sql() {
      return sb.toString();
    }
  }
}
