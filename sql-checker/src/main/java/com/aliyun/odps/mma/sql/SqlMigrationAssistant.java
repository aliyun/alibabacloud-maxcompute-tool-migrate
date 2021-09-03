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
