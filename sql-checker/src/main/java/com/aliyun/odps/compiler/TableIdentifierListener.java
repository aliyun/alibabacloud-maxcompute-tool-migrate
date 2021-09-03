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

package com.aliyun.odps.compiler;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.compiler.parser.OdpsParser.CteStatementContext;
import com.aliyun.odps.compiler.parser.OdpsParser.TableNameContext;
import com.aliyun.odps.compiler.parser.OdpsParserBaseListener;
import com.aliyun.odps.mma.sql.HiveMetaCache;

public class TableIdentifierListener extends OdpsParserBaseListener {

  private static final Logger LOG = LogManager.getLogger(TableIdentifierListener.class);

  private String defaultProjectName;
  private Set<String> identifiers = new HashSet<>();
  private HiveMetaCache hiveMetaCache;

  public TableIdentifierListener(String defaultProjectName, HiveMetaCache hiveMetaCache) {
    this.defaultProjectName = Objects.requireNonNull(defaultProjectName);
    this.hiveMetaCache = Objects.requireNonNull(hiveMetaCache);
  }

  @Override
  public void exitCteStatement(CteStatementContext ctx) {
    super.exitCteStatement(ctx);
    identifiers.add(ctx.identifier().getText());
  }

  @Override
  public void exitTableName(TableNameContext ctx) {
    super.exitTableName(ctx);

    String db = defaultProjectName;
    String tab;
    if (ctx.db != null && !StringUtils.isBlank(ctx.db.getText())) {
      db = ctx.db.getText();
    }
    tab = ctx.tab.getText();

    try {
      hiveMetaCache.cache(db, tab);
    } catch (Exception e) {
      // Handle identifiers defined in the cte statements
      if (!(e instanceof NoSuchObjectException)
          || ctx.db != null
          || !identifiers.contains(tab)) {
        throw new RuntimeException(e);
      }
    }
  }
}
