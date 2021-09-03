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

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.compiler.util.HashMapBuilder;
import com.aliyun.odps.compiler.util.IOUtil;

import java.io.File;
import java.util.Map;

/**
 * Descriptions of odps meta source.
 * All instances should be built from factories
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public abstract class Meta {

  // Sealed
  private Meta() {}

  /**
   * Providing meta from ddl string.
   * Multiple ddl statements are separated by SEMI-COLON (;)
   * The USE command (use <project_name>;) is available to switch projects
   * Resources for udf should be included in class path
   *
   * Note that parsing a very large ddl string (> 10MB) is low efficient
   *
   * @param ddl the ddl string.
   * @return a Meta object
   */
  public static Meta ddl(String ddl) {
    try {
      File f = File.createTempFile("migration_tool_ddl", "ddl");
      IOUtil.writeFullText(f, ddl);
      return ddlFile(f.getAbsolutePath());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Providing meta from ddl file which contains ddl statements
   * Multiple ddl statements are separated by SEMI-COLON (;)
   * The USE command (use <project_name>;) is available to switch projects
   * Resources for udf should be included in class path
   *
   * Note that parsing a very large ddl file (> 10MB) is low efficient
   *
   * @param files the ddl file paths. If there are multiple files, separated by File.pathSeparator
   * @return a Meta object
   */
  public static Meta ddlFile(String files) {
    return new DdlMeta(files);
  }

  /**
   * Providing meta from a directory organized as designed.
   * The directory should be organized as following:
   *
   * ${ROOT}
   *   ${project name 1}
   *     tables
   *       ${table name 1}
   *         schema.ddl  -- example content: CREATE TABLE IF NOT EXISTS ${table name} (...);
   *       ${table name 2}
   *         schema.ddl
   *       ...
   *     functions
   *       ${function name 1}.ddl  -- example content: CREATE FUNCTION ${function name} AS '...' using '...';
   *       ${function name 2}.ddl
   *       ...
   *     resources
   *       ${resource name 1}.ddl   -- example content: ADD JAR '<PATH TO JAR FILE>';
   *       ${resource name 2}.ddl
   *       ...
   *   ${project name 2}
   *     ...
   *   ...
   *
   * Note that this can have higher efficiency than parsing a single ddl file or ddl string
   *
   * @param path Path to ${ROOT}
   * @return a Meta object
   */
  public static Meta directory(String path) {
    return new FileSystemMeta(path);
  }

  /**
   * Providing meta from sdk
   * @param odps the sdk odps object
   * @return a Meta object
   */
  public static Meta connect(Odps odps) {
    return new SdkMeta(odps);
  }

  //////////////////////
  // the following is for internal implementations
  //////////////////////

  abstract String provider();
  abstract Map<String, String> properties();

  private static class SdkMeta extends Meta {

    private final Odps odps;

    SdkMeta(Odps odps) {
      this.odps = odps;
    }

    @Override
    String provider() {
      return "odps-sdk";
    }

    @Override
    Map<String, String> properties() {
      return new HashMapBuilder<String, String>()
          .with("access_id", ((AliyunAccount) odps.getAccount()).getAccessId())
          .with("access_key", ((AliyunAccount) odps.getAccount()).getAccessKey())
          .with("end_point", odps.getEndpoint())
          .with("project_name", odps.getDefaultProject() == null ? "ignored" : odps.getDefaultProject())
          .build();
    }
  }

  private static class FileSystemMeta extends Meta {

    private final String path;

    public FileSystemMeta(String path) {
      this.path = path;
    }

    @Override
    String provider() {
      return "odpsql-fs";
    }

    @Override
    Map<String, String> properties() {
      return new HashMapBuilder<String, String>()
          .with("local_root", this.path)
          .with("project_name", "ignored").build();
    }
  }

  private static class DdlMeta extends Meta {

    private final String file;

    public DdlMeta(String file) {
      this.file = file;
    }

    @Override
    String provider() {
      return "odpsql-ddl";
    }

    @Override
    Map<String, String> properties() {
      return new HashMapBuilder<String, String>().with("ddl_files", file).build();
    }
  }
}
