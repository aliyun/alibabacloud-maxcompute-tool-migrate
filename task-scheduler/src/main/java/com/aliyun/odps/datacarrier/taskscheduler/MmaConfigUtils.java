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

package com.aliyun.odps.datacarrier.taskscheduler;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.BasicConfigurator;
import org.apache.tools.ant.filters.StringInputStream;

import com.aliyun.odps.utils.StringUtils;
import com.csvreader.CsvReader;

public class MmaConfigUtils {
  public static final Map<String, String> HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS = new HashMap();
  public static final MmaConfig.SQLSettingConfig HIVE_SOURCE_TABLE_DEFAULT_CONFIG;
  public static final MmaConfig.AdditionalTableConfig DEFAULT_ADDITIONAL_TABLE_CONFIG;
  public static final MmaConfig.HiveConfig SAMPLE_HIVE_CONFIG;
  public static final MmaConfig.OdpsConfig SAMPLE_ODPS_CONFIG;
  public static final MmaConfig.MetaDbConfig SAMPLE_META_DB_CONFIG;
  private static final Pattern TABLE_MAPPING_LINE_PATTERN;

  static {
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("hive.fetch.task.conversion", "none");
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("hive.execution.engine", "mr");
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put(
        "mapreduce.max.split.size",
        Integer.toString(Constants.DEFAULT_MAPREDUCE_SPLIT_SIZE_IN_BYTE));
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("mapreduce.task.timeout", "3600000");
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("mapreduce.map.maxattempts", "0");
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("mapreduce.map.speculative", "false");
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("mapreduce.map.cpu.vcores", "1");
    HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS.put("mapreduce.map.memory.mb", "4096");

    HIVE_SOURCE_TABLE_DEFAULT_CONFIG =
        new MmaConfig.SQLSettingConfig(null, HIVE_TO_MC_DEFAULT_MIGRATION_SETTINGS, null);
    DEFAULT_ADDITIONAL_TABLE_CONFIG =
        new MmaConfig.AdditionalTableConfig(200, 1);
    SAMPLE_HIVE_CONFIG = new MmaConfig.HiveConfig(
        "jdbc:hive2://127.0.0.1:10000/default",
        "Hive",
        "",
        "thrift://127.0.0.1:9083",
        null,
        null,
        null,
        Collections.emptyMap(),
        HIVE_SOURCE_TABLE_DEFAULT_CONFIG);
    SAMPLE_ODPS_CONFIG = new MmaConfig.OdpsConfig(
        "access ID",
        "access key",
        "endpoint",
        "project name");
    SAMPLE_META_DB_CONFIG = new MmaConfig.MetaDbConfig(
        "org.h2.Driver",
        "jdbc:h2:file:/tmp/mma/.MmaMeta;AUTO_SERVER=TRUE",
        "mma",
        "mma",
        20);

    /**
     * Acceptable formats:
     *   source_db.source_tbl:dest_db.dest_tbl
     *   source_db.source_tbl("pt_val1", "pt_val2", ...):dest_db.dest_tbl
     */
    TABLE_MAPPING_LINE_PATTERN = Pattern.compile("([^()]+)\\.([^()]+)(\\([^()]+\\))?:([^()]+)\\.([^()]+)");
  }

  public static MmaConfig.HiveConfig parseHiveConfig(Path hiveConfigPath) throws IOException {
    if (hiveConfigPath == null || !hiveConfigPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid path");
    }

    Properties properties = new Properties();
    properties.load(new FileReader(hiveConfigPath.toFile()));
    return new MmaConfig.HiveConfig(
        properties.getProperty("jdbc_connection_url"),
        properties.getProperty("user"),
        properties.getProperty("password"),
        properties.getProperty("hms_thrift_addr"),
        null,
        null,
        null,
        Collections.emptyMap(),
        HIVE_SOURCE_TABLE_DEFAULT_CONFIG);
  }

  public static MmaConfig.OdpsConfig parseOdpsConfig(Path odpsConfigPath) throws IOException {
    if (odpsConfigPath == null || !odpsConfigPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid path");
    }

    Properties properties = new Properties();
    properties.load(new FileReader(odpsConfigPath.toFile()));

    return new MmaConfig.OdpsConfig(
        properties.getProperty("access_id"),
        properties.getProperty("access_key"),
        properties.getProperty("end_point"),
        properties.getProperty("project_name"));
  }

  public static MmaConfig.MetaDbConfig parseMetaDBConfig(Path metaDbConfigPath) throws IOException {
    if (metaDbConfigPath == null) {
      // Ensure MMA_HOME is set
      String mmaHome = System.getenv("MMA_HOME");
      if (mmaHome == null) {
        throw new IllegalStateException("Environment variable 'MMA_HOME' not set");
      }
      Path parentDir = Paths.get(mmaHome);
      String connectionUrl = "jdbc:h2:file:"
          + Paths.get(parentDir.toString(), Constants.DB_FILE_NAME).toAbsolutePath()
          + ";AUTO_SERVER=TRUE";
      return new MmaConfig.MetaDbConfig("h2", connectionUrl, "mma", "mma", 50);
    }

    if (!metaDbConfigPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid meta db config path");
    }

    Properties properties = new Properties();
    properties.load(new FileReader(metaDbConfigPath.toFile()));
    String userPropertyValue = properties.getProperty("user", "");
    if (StringUtils.isNullOrEmpty(userPropertyValue)) {
      userPropertyValue = System.getenv("META_DB_USER");
    }
    String passwordPropertyValue = properties.getProperty("password", "");
    if (StringUtils.isNullOrEmpty(passwordPropertyValue)) {
      passwordPropertyValue = System.getenv("META_DB_PASSWORD");
    }
    if (StringUtils.isNullOrEmpty(userPropertyValue) || StringUtils.isNullOrEmpty(passwordPropertyValue)) {
      throw new IllegalArgumentException("Invalid meta db user and password");
    }
    return new MmaConfig.MetaDbConfig(
        properties.getProperty("db_type"),
        properties.getProperty("jdbc_connection_url"),
        userPropertyValue,
        passwordPropertyValue,
        Integer.parseInt(properties.getProperty("max_pool_size")));
  }

  public static List parseTableMapping(
      List<String> lines) throws IOException {
    // Used to remove duplications
    Map<String, Map<String, MmaConfig.TableMigrationConfig>> dbToTableToConfig =
        new LinkedHashMap<>();

    for (String line : lines) {
      // Skip empty line and comments
      if (line == null || line.trim().isEmpty() || line.startsWith("#")) {
        continue;
      }

      line = line.trim();

      Matcher m = TABLE_MAPPING_LINE_PATTERN.matcher(line);
      if (m.matches()) {
        if (m.groupCount() != 5) {
          System.err.println("[ERROR] Invalid line: " + line);
          continue;
        }
        String sourceDb = Objects.requireNonNull(m.group(1));
        String sourceTbl = Objects.requireNonNull(m.group(2));
        String destDb = Objects.requireNonNull(m.group(4));
        String destTbl = Objects.requireNonNull(m.group(5));
        List<String> partitionValues = null;
        if (m.group(3) != null) {
          // Remove parentheses
          String partitionValuesStr = m.group(3).substring(1, m.group(3).length() - 1);
          StringInputStream sis = new StringInputStream(partitionValuesStr);
          CsvReader csvReader = new CsvReader(sis, StandardCharsets.UTF_8);
          if (csvReader.readRecord()) {
            partitionValues = new LinkedList<>(Arrays.asList(csvReader.getValues()));
          } else {
            System.err.println("[ERROR] Invalid partition values: " + m.group(3));
            continue;
          }
        }

        Map<String, MmaConfig.TableMigrationConfig> tblToConfig =
            dbToTableToConfig.computeIfAbsent(sourceDb, s -> new LinkedHashMap());
        if (!tblToConfig.containsKey(sourceTbl)) {
          MmaConfig.TableMigrationConfig tableMigrationConfig = new MmaConfig.TableMigrationConfig(
              DataSource.Hive,
              sourceDb,
              sourceTbl,
              destDb,
              destTbl,
              DEFAULT_ADDITIONAL_TABLE_CONFIG);

          tblToConfig.put(sourceTbl, tableMigrationConfig);
        }

        MmaConfig.TableMigrationConfig tableMigrationConfig = tblToConfig.get(sourceTbl);

        if (!destDb.equalsIgnoreCase(tableMigrationConfig.getDestProjectName()) ||
            !destTbl.equalsIgnoreCase(tableMigrationConfig.getDestTableName())) {
          System.err.println("[ERROR] Source table mapped to multiple dest tables: " + line);
          continue;
        }
        if (partitionValues != null) {
          tableMigrationConfig.addPartitionValues(partitionValues);
        }
      } else {
        System.err.println("[WARN] Invalid line: " + line);
      }
    }

    return dbToTableToConfig
        .values()
        .stream()
        .flatMap(v -> v.values().stream())
        .collect(Collectors.toList());
  }

  public static List<MmaConfig.TableMigrationConfig> parseTableMapping(
      Path tableMappingPath) throws IOException {
    if (tableMappingPath == null || !tableMappingPath.toFile().exists()) {
      throw new IllegalArgumentException("Invalid path");
    }

    List<String> lines = DirUtils.readLines(tableMappingPath);
    return parseTableMapping(lines);
  }

  private static void help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "generate-config";
    formatter.printHelp(cmdLineSyntax, options);
  }

  public static void generateMmaServerConfig(
      Path hiveConfigPath,
      Path odpsConfigPath,
      Path metaDBConfigPath, String prefix) throws IOException {
    String json = new MmaServerConfig(
        DataSource.Hive,
        null,
        parseHiveConfig(hiveConfigPath),
        parseOdpsConfig(odpsConfigPath),
        parseMetaDBConfig(metaDBConfigPath),
        null, Collections.emptyMap(), null).toJson();
    DirUtils.writeFile(Paths.get(prefix + "mma_server_config.json"), json);
  }

  public static void generateMmaMigrationConfig(
      Path tableMappingPath, String prefix) throws IOException {
    String json = new MmaMigrationConfig(
        "Jerry",
        parseTableMapping(tableMappingPath),
        DEFAULT_ADDITIONAL_TABLE_CONFIG).toJson();
    DirUtils.writeFile(Paths.get(prefix + "mma_migration_config.json"), json);
  }

  public static void generateSampleConfigs() throws IOException {
   MmaServerConfig mmaServerConfig = new MmaServerConfig(
       DataSource.Hive,
       null,
       SAMPLE_HIVE_CONFIG,
       SAMPLE_ODPS_CONFIG,
       SAMPLE_META_DB_CONFIG,
       null,
       Collections.emptyMap(),
       null);

    MmaConfig.TableMigrationConfig tableMigrationConfig = new MmaConfig.TableMigrationConfig(
        DataSource.Hive,
        "source DB",
        "source table",
        "dest project",
        "dest table",
        DEFAULT_ADDITIONAL_TABLE_CONFIG);

    MmaMigrationConfig mmaMigrationConfig = new MmaMigrationConfig(
        "Jerry",
        Collections.singletonList(tableMigrationConfig),
        DEFAULT_ADDITIONAL_TABLE_CONFIG);

    DirUtils.writeFile(Paths.get("mma_server_config.json"), mmaServerConfig.toJson());
    DirUtils.writeFile(Paths.get("mma_client_config.json"), mmaServerConfig.toJson());
    DirUtils.writeFile(Paths.get("mma_migration_config.json"), mmaMigrationConfig.toJson());
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    Option helpOption = Option
        .builder("h")
        .longOpt(Constants.HELP)
        .argName(Constants.HELP)
        .desc("print help information")
        .build();

    Option sampleOption = Option
        .builder()
        .longOpt("sample")
        .hasArg(false)
        .desc("generate sample server, client and migration configs")
        .build();

    Option serverOption = Option
        .builder("s")
        .longOpt("to_server_config")
        .hasArg(false)
        .desc("convert hive_config.ini specified by '--hive_config' and "
                  + "odps_config.ini specified by '--odps_config' to mma server config")
        .build();

    Option migrationOption = Option
        .builder("m")
        .longOpt("to_migration_config")
        .hasArg(false)
        .desc("convert table mapping specified by '--table_mapping' to mma migration config")
        .build();

    Option hiveConfigOption = Option
        .builder()
        .longOpt("hive_config")
        .hasArg()
        .argName("hive_config.ini path")
        .desc("hive_config.ini path")
        .build();

    Option odpsConfigOption = Option
        .builder()
        .longOpt("odps_config")
        .hasArg()
        .argName("odps_config.ini path")
        .desc("odps_config.ini path")
        .build();

    Option metaDBConfigOption = Option
        .builder()
        .longOpt("meta_db_conf")
        .hasArg()
        .argName("meta_db_config.ini path")
        .desc("meta_db_config.ini path")
        .build();

    Option tableMappingOption = Option
        .builder()
        .longOpt("table_mapping")
        .hasArg()
        .argName("table_mapping.txt path")
        .desc("table_mapping.txt path")
        .build();

    Option prefixOption = Option
        .builder("p")
        .longOpt("prefix")
        .hasArg()
        .argName("prefix")
        .desc("prefix for generated files")
        .build();

    Options options = new Options()
        .addOption(helpOption)
        .addOption(sampleOption)
        .addOption(serverOption)
        .addOption(migrationOption)
        .addOption(hiveConfigOption)
        .addOption(odpsConfigOption)
        .addOption(metaDBConfigOption)
        .addOption(tableMappingOption)
        .addOption(prefixOption);

    DefaultParser defaultParser = new DefaultParser();
    CommandLine cmd = defaultParser.parse(options, args);

    if (cmd.hasOption(Constants.HELP)) {
      help(options);
      System.exit(0);
    }

    if (cmd.hasOption("sample")) {
      generateSampleConfigs();
      System.exit(0);
    }

    String prefix = "";
    if (cmd.hasOption("prefix")) {
      prefix = cmd.getOptionValue("prefix").trim();
    }

    if (cmd.hasOption("to_server_config")) {
      if (!cmd.hasOption("hive_config") || !cmd.hasOption("odps_config")) {
        throw new IllegalArgumentException("Requires '--hive_config' and '--odps_config'");
      }

      String metaDbConfPathString = cmd.getOptionValue("meta_db_conf");

      generateMmaServerConfig(
          Paths.get(cmd.getOptionValue("hive_config")),
          Paths.get(cmd.getOptionValue("odps_config")),
          metaDbConfPathString != null ? Paths.get(metaDbConfPathString) : null, prefix);
    }

    if (cmd.hasOption("to_migration_config")) {
      if (!cmd.hasOption("table_mapping")) {
        throw new IllegalArgumentException("Requires '--table_mapping'");
      }

      generateMmaMigrationConfig(Paths.get(cmd.getOptionValue("table_mapping")), prefix);
    }
  }
}
