package com.aliyun.odps.mma.sql;

import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.aliyun.odps.compiler.TableIdentifierListener;
import com.aliyun.odps.compiler.parser.OdpsLexer;
import com.aliyun.odps.compiler.parser.OdpsParser;

public class SqlMigrationAssistant {

  private String defaultProject;
  private HiveMetaCache hiveMetaCache;
  private Map<String, String> defaultSettings;

  public SqlMigrationAssistant(
      String defaultProject,
      Map<String, String> defaultSettings,
      HiveMetaCache hiveMetaCache) {
    this.defaultProject = defaultProject;
    this.defaultSettings = defaultSettings;
    this.hiveMetaCache = hiveMetaCache;
  }

  /**
   * Walk through the abstract syntax tree, and load necessary metadata into the cache.
   * @param sql The SQL statement.
   */
  public void prepareMetadata(String sql) {
    ANTLRInputStream input = new ANTLRInputStream(sql);
    OdpsLexer lexer = new OdpsLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    OdpsParser parser = new OdpsParser(tokens);
    ParseTreeWalker treeWalker = new ParseTreeWalker();
    treeWalker.walk(new TableIdentifierListener(defaultProject, hiveMetaCache), parser.script());
  }

  /**
   * Check whether a query is compatible.
   * @param sql The SQL statement
   * @return {@link CompatibilityDescription}
   */
  public CompatibilityDescription check(
      String sql,
      Map<String, String> settings,
      String hiveMetadataCachePath) {
    Meta meta = Meta.directory(hiveMetadataCachePath);
    SqlCompatibilityChecker checker =
        SqlCompatibilityChecker.create(defaultProject, meta, defaultSettings);
    return checker.check(sql, settings);
  }
}
