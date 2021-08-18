package com.aliyun.odps.compiler;

public class CaseDetector extends TreeScanner<Void, Void> {

  public interface Collector {
    void collect(String key, SyntaxTreeNode ast);
  }

  private Collector collector;

  public void detect(SyntaxTreeNode ast, Collector collector) {
    this.collector = collector;
    scan(ast, null);
  }

  void emit(String key, SyntaxTreeNode ast) {
    collector.collect(key, ast);
  }
}
