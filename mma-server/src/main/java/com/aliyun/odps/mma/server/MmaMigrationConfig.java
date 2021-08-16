/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.mma.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.odps.mma.util.GsonUtils;

public class MmaMigrationConfig implements MmaConfig.Config {
  private String user;
  private MmaConfig.AdditionalTableConfig globalAdditionalTableConfig;
  private MmaConfig.ServiceMigrationConfig serviceMigrationConfig;
  private List<MmaConfig.DatabaseMigrationConfig> databaseMigrationConfigs;
  private List<MmaConfig.TableMigrationConfig> tableMigrationConfigs;
  private List<MmaConfig.ObjectBackupConfig> objectBackupConfigs;
  private List<MmaConfig.ObjectRestoreConfig> objectRestoreConfigs;
  private List<MmaConfig.DatabaseBackupConfig> databaseBackupConfigs;
  private List<MmaConfig.DatabaseRestoreConfig> databaseRestoreConfigs;

  public MmaMigrationConfig(
      String user,
      List<MmaConfig.TableMigrationConfig> tableMigrationConfigs,
      MmaConfig.AdditionalTableConfig globalAdditionalTableConfig) {
    this.user = user;
    this.tableMigrationConfigs = tableMigrationConfigs;
    this.globalAdditionalTableConfig = globalAdditionalTableConfig;
  }

  @Override
  public boolean validate() {
    boolean valid = true;
    if (serviceMigrationConfig != null) {
      if (databaseMigrationConfigs != null && !databaseMigrationConfigs.isEmpty()
          || tableMigrationConfigs != null && !tableMigrationConfigs.isEmpty()) {
        throw new IllegalArgumentException(
            "Service migration config exists, please remove database and table migration configs");
      }
      valid = serviceMigrationConfig.validate();
    } else if (databaseMigrationConfigs != null) {
      if (tableMigrationConfigs != null && !tableMigrationConfigs.isEmpty()) {
        throw new IllegalArgumentException(
            "Database migration config exists, please remove table migration configs");
      }
      valid =
          databaseMigrationConfigs.stream().allMatch(MmaConfig.DatabaseMigrationConfig::validate);
    } else if (tableMigrationConfigs != null) {
      valid = tableMigrationConfigs.stream().allMatch(MmaConfig.TableMigrationConfig::validate);
    } else if (objectBackupConfigs != null) {
      valid = objectBackupConfigs.stream().allMatch(MmaConfig.ObjectBackupConfig::validate);
    } else if (objectRestoreConfigs != null) {
      // TODO: validate restore configs
    } else if (databaseBackupConfigs != null) {
      valid = databaseBackupConfigs.stream().allMatch(MmaConfig.DatabaseBackupConfig::validate);
    } else if (databaseRestoreConfigs != null) {
      valid = databaseRestoreConfigs.stream().allMatch(MmaConfig.DatabaseRestoreConfig::validate);
    } else {
      throw new IllegalArgumentException("No migration config found");
    }

    return valid && (globalAdditionalTableConfig == null || globalAdditionalTableConfig.validate());
  }

  public String getUser() {
    return user;
  }

  public MmaConfig.ServiceMigrationConfig getServiceMigrationConfig() {
    return serviceMigrationConfig;
  }

  public List<MmaConfig.DatabaseMigrationConfig> getDatabaseMigrationConfigs() {
    return databaseMigrationConfigs;
  }

  public List<MmaConfig.TableMigrationConfig> getTableMigrationConfigs() {
    return tableMigrationConfigs;
  }

  public List<MmaConfig.ObjectBackupConfig> getObjectBackupConfigs() {
    return objectBackupConfigs;
  }

  public List<MmaConfig.ObjectRestoreConfig> getObjectRestoreConfigs() {
    return objectRestoreConfigs;
  }

  public List<MmaConfig.DatabaseBackupConfig> getDatabaseBackupConfigs() {
    return databaseBackupConfigs;
  }

  public List<MmaConfig.DatabaseRestoreConfig> getDatabaseRestoreConfigs() {
    return databaseRestoreConfigs;
  }

  public MmaConfig.AdditionalTableConfig getGlobalAdditionalTableConfig() {
    return globalAdditionalTableConfig;
  }

  public String toJson() {
    return GsonUtils.GSON.toJson(this);
  }

  public static MmaMigrationConfig fromFile(Path path) throws IOException {
    if (!path.toFile().exists()) {
      throw new IllegalArgumentException("File not found: " + path);
    }

    String content = DirUtils.readFile(path);
    return GsonUtils.GSON.fromJson(content, MmaMigrationConfig.class);
  }
}