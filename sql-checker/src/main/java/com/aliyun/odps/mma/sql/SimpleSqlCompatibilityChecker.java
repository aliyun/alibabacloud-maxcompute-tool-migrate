package com.aliyun.odps.mma.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.odps.compiler.CaseDetector;
import com.aliyun.odps.compiler.IssueDetector;
import com.aliyun.odps.compiler.OdpsQlCompiler;
import com.aliyun.odps.compiler.SimpleScratchDir;
import com.aliyun.odps.compiler.compiler.CodeOutput;
import com.aliyun.odps.compiler.compiler.CompileCommand;
import com.aliyun.odps.compiler.compiler.CompilerOutput;
import com.aliyun.odps.compiler.compiler.StringCompilerInput;

class SimpleSqlCompatibilityChecker extends SqlCompatibilityChecker {

  private Properties props = new Properties();
  private CompileCommand command = new CompileCommand();

  SimpleSqlCompatibilityChecker(String projectName, Meta meta, Map<String, String> defaultSettings) {
    super(meta, defaultSettings);
    if (defaultSettings != null) {
      defaultSettings.forEach(props::setProperty);
    }
    command.setDefaultProject(projectName)
           .setScratchDir(new SimpleScratchDir(System.getProperty("java.io.tmpdir")))
           .setOdps(meta.provider(), meta.properties())
           .setFormat("pot")
           .setOutput(new DummyCompilerOutput());
  }

  @Override
  public CompatibilityDescription check(String sql, Map<String, String> settings) {
    command.setInput(new StringCompilerInput("input", sql));
    command.setProperties(props);
    if (settings != null) {
      this.props = new Properties(props);
      settings.forEach(command::setProperty);
    }

    final List<Issue> issues = new ArrayList<>();
    SqlPreParser.preParse(sql, issues);
    issues.addAll(checkInternal(command));

    return new CompatibilityDescription() {
      @Override
      public List<Issue> getIssues() {
        return issues;
      }

      @Override
      public String transform() {
        return SimpleSqlCompatibilityChecker.this.subst(sql, issues);
      }
    };
  }

  private String subst(String sql, List<Issue> issues) {

    int prevCount = -1;
    boolean needNextIter;
    do {
      needNextIter = false;

      List<SubstIssue> collect = issues.stream().filter(a -> a instanceof SubstIssue)
                                       .map(a-> ((SubstIssue) a)).sorted(Comparator.comparing(SubstIssue::start).reversed()).collect(Collectors.toList());
      if (prevCount > 0 && collect.size() > prevCount) {
        throw new UnsupportedOperationException("cannot automatically transform this sql");
      }
      prevCount = collect.size();

      int start = -1;
      for (SubstIssue issue : collect) {
        if (start >= 0 && issue.end() > start) {
          needNextIter = true;
          continue;
        }
        start = issue.start();
        sql = issue.subst(sql);
      }

      if (needNextIter) {
        command.setInput(new StringCompilerInput("input", sql));
        issues = checkInternal(command);
      }
    } while (needNextIter);
    return sql;
  }

  private List<Issue> checkInternal(CompileCommand command) {

    List<Issue> issues;
    OdpsQlCompiler compiler = new OdpsQlCompiler();
    LogItemIssue.IssueLogger logger = new LogItemIssue.IssueLogger();

    try {
      compiler.compile(command, logger);
      issues = new ArrayList<>(logger.getIssues(compiler.getAst()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    CaseDetector.Collector collector = new IssueCollector(issues);
    for (CaseDetector detector : IssueDetector.getDetectors()) {
      detector.detect(compiler.getAst(), collector);
    }
    return issues;
  }

  private class DummyCompilerOutput implements CompilerOutput {
    @Override
    public void addOutput(String s, CodeOutput codeOutput) {
    }

    @Override
    public CodeOutput getCodeOutput(String s) {
      return null;
    }
  }
}
