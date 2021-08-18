package com.aliyun.odps.mma.sql;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;

import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.meta.transform.SchemaTransformer.SchemaTransformResult;
import com.aliyun.odps.mma.meta.transform.SchemaTransformerFactory;
import com.aliyun.odps.mma.util.McSqlUtils;

public class HiveMetaCache {

  private MetaSource hiveMetaSource;
  private Path ddlDir;
  private JobConfiguration config;

  // For testing
  public HiveMetaCache() {
  }

  public HiveMetaCache(MetaSource hiveMetaSource, Path ddlDir, JobConfiguration config) {
    this.hiveMetaSource = hiveMetaSource;
    this.ddlDir = ddlDir;
    this.config = config;
  }

  public void cache(String database, String table) throws Exception {
    if (isCached(database, table)) {
      return;
    }

    TableMetaModel tableMetaModel = hiveMetaSource.getTableMeta(database, table);
    SchemaTransformResult schemaTransformResult = SchemaTransformerFactory
        .get(DataSourceType.Hive)
        .transform(tableMetaModel, config);
    TableMetaModel mcTableMetaModel = schemaTransformResult.getTableMetaModel();
    String ddl = McSqlUtils.getCreateTableStatement(mcTableMetaModel);
    Path tableDdlDirPath = Paths.get(ddlDir.toString(), database, "tables", table);
    Files.createDirectories(tableDdlDirPath);
    Path tableDdlFilePath = Paths.get(tableDdlDirPath.toString(), "schema.ddl");
    File tableDdlFile = new File(tableDdlFilePath.toString());
    IOUtils.write(ddl, new FileOutputStream(tableDdlFile), StandardCharsets.UTF_8);
  }

  public boolean isCached(String database, String table) {
    Path metaPath = Paths.get(ddlDir.toString(), database, "tables", table, "schema.ddl");
    return Files.exists(metaPath);
  }
}
