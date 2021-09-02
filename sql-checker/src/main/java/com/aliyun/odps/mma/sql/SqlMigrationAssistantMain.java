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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.ResourceBundle;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.odps.mma.sql.utils.PrintUtils;
import com.aliyun.odps.mma.util.GsonUtils;

public class SqlMigrationAssistantMain {

  private static final Logger LOG = LogManager.getLogger(SqlMigrationAssistantMain.class);

  private static ResourceBundle cases = ResourceBundle.getBundle("cases");

  private static final int QUERY_TRUNCATE_SIZE = 128;

  private static final String USAGE = "sql-checker [-d | -f | -q] [-s]";
  private static final String QUERY_OPTION = "q";
  private static final String QUERY_LONG_OPTION = "query";
  private static final String FILE_OPTION = "f";
  private static final String FILE_LONG_OPTION = "file";
  private static final String DIR_OPTION = "d";
  private static final String DIR_LONG_OPTION = "directory";
  private static final String SETTINGS_OPTION = "s";
  private static final String SETTINGS_LONG_OPTION = "settings";
  private static final String HELP_OPTION = "h";
  private static final String HELP_LONG_OPTION = "help";

  private enum IssueCategory {
    /**
     * Error
     */
    ERROR {
      @Override
      public String toString() {
        return "General issue (error)";
      }
    },

    /**
     * Strong warning
     */
    STRONG_WARNING {
      @Override
      public String toString() {
        return "General issue (strong warning)";
      }
    },

    /**
     * Weak warning
     */
    WEAK_WARNING {
      @Override
      public String toString() {
        return "General issue (weak warning)";
      }
    },

    UDF {
      @Override
      public String toString() {
        return "UDF issue";
      }
    },
    DECIMAL {
      @Override
      public String toString() {
        return "Decimal issue";
      }
    };

    @Override
    public abstract String toString();
  }

  private static class ScriptOrDirCompatibilityDescription {
    private String inputPath;
    private int numOfQueries;
    private Map<CompatibilityLevel, Integer> compatibilityLevelToQueryCount;

    ScriptOrDirCompatibilityDescription(
        String inputPath,
        int numOfQueries,
        Map<CompatibilityLevel, Integer> compatibilityLevelToQueryCount) {
      this.inputPath = inputPath;
      this.numOfQueries = numOfQueries;
      this.compatibilityLevelToQueryCount = compatibilityLevelToQueryCount;
    }

    public String getInputPath() {
      return inputPath;
    }

    public int getNumOfQueries() {
      return numOfQueries;
    }

    public Map<CompatibilityLevel, Integer> getCompatibilityLevelToQueryCount() {
      return compatibilityLevelToQueryCount;
    }

    public void print() {
      PrintUtils.println(
          "Number of queries: " + numOfQueries, System.err);
      StringBuilder compatibilityBuilder = new StringBuilder();
      for (CompatibilityLevel level : CompatibilityLevel.values()) {
        if (compatibilityLevelToQueryCount.containsKey(level)) {
          compatibilityBuilder
              .append(level.toString()).append("(").append(compatibilityLevelToQueryCount.get(level)).append(") ");
        }
      }
      PrintUtils.println(
          "Compatibility: " + compatibilityBuilder.toString(), System.err);

      StringBuilder compatibilityRatioBuilder = new StringBuilder();
      for (CompatibilityLevel level : CompatibilityLevel.values()) {
        if (compatibilityLevelToQueryCount.containsKey(level)) {
          compatibilityRatioBuilder
              .append(level.toString())
              .append("(")
              .append(String.format("%.2f%%", 100 * ((double) compatibilityLevelToQueryCount.get(level)) / numOfQueries))
              .append(") ");
        }
      }
      if (numOfQueries != 0) {
        PrintUtils.println("Compatibility ratio: " + compatibilityRatioBuilder, System.err);
      } else {
        PrintUtils.println("Compatible ratio: N/A", System.err);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = initOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP_OPTION) || cmd.hasOption(HELP_LONG_OPTION)) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(USAGE, options);
      System.exit(0);
    }

    if (!cmd.hasOption(QUERY_OPTION) && !cmd.hasOption(FILE_OPTION) && !cmd.hasOption(DIR_OPTION)) {
      String errMsg = "One of the following options is required: "
          + String.join(", ", QUERY_LONG_OPTION, FILE_LONG_OPTION, DIR_LONG_OPTION);
      throw new IllegalArgumentException(errMsg);
    }

    Map<String, String> settings = new HashMap<>();
    if (cmd.hasOption(SETTINGS_OPTION)) {
      settings = parseSettings(cmd.getOptionValue(SETTINGS_OPTION));
    }

    String mmaHome = System.getenv("MMA_HOME");
    if (mmaHome == null) {
      throw new IllegalStateException("Environment variable 'MMA_HOME' not set");
    }

    // The metadata cache dir is fixed to ${MMA_HOME}/tmp/ddl
    Path ddlDir = Paths.get(mmaHome, "tmp", "meta_cache");
    PrintUtils.printlnYellow("(1/4) Loading cache directory: " + ddlDir.toFile(), System.err);
    Files.createDirectories(ddlDir);
    PrintUtils.printlnGreen("Succeeded", System.err);

    // The hive configuration file path is fixed to ${MMA_HOME}/conf/hive_config.ini
    Path mmaServerConfigurationPath = Paths.get(mmaHome, "conf", "mma_server_config.json");
    PrintUtils.printlnYellow(
        "(2/4) Loading Hive configuration from: " + mmaServerConfigurationPath.toFile(),
        System.err);
    String json = IOUtils.toString(mmaServerConfigurationPath.toUri(), StandardCharsets.UTF_8);
    JobConfiguration config = JobConfiguration.fromJson(json);
    PrintUtils.printlnGreen("Succeeded", System.err);

    PrintUtils.printlnYellow("(3/4) Connecting to Hive MetaStore", System.err);
    MetaSourceFactory metaSourceFactory = new MetaSourceFactory();
    MetaSource metaSource = metaSourceFactory.getMetaSource(config);
    PrintUtils.printlnGreen("Connected", System.err);

    HiveMetaCache hiveMetaCache = new HiveMetaCache(metaSource, ddlDir, config);
    SqlMigrationAssistant sqlMigrationAssistant =
        new SqlMigrationAssistant("default", new HashMap<>(), hiveMetaCache);

    PrintUtils.printlnYellow("(4/4) Checking query compatibility", System.err);
    if (cmd.hasOption(QUERY_OPTION)) {
      String query = cmd.getOptionValue(QUERY_OPTION).trim();
      check(
          null,
          // Hard coded index since there is only one query
          1,
          1,
          query,
          settings,
          sqlMigrationAssistant,
          ddlDir.toString(),
          System.err,
          System.out,
          System.err);
    } else if (cmd.hasOption(FILE_OPTION)) {
      long time = System.currentTimeMillis();
      String outputPath = Paths.get(
          System.getProperty("user.dir"), "result_" + time + ".txt").toString();
      String errorOutputPath = Paths.get(
          System.getProperty("user.dir"), "error_" + time + ".txt").toString();
      String inputPath = new File(cmd.getOptionValue(FILE_OPTION).trim()).getAbsolutePath();
      PrintStream outputPrintStream = new PrintStream(new FileOutputStream(new File(outputPath)));
      PrintStream errorOutputPrintStream =
          new PrintStream(new FileOutputStream(new File(errorOutputPath)));

      ScriptOrDirCompatibilityDescription scriptDesc = checkScript(
          inputPath,
          settings,
          sqlMigrationAssistant,
          ddlDir.toString(),
          outputPath,
          errorOutputPath,
          System.err,
          outputPrintStream,
          errorOutputPrintStream);

      PrintUtils.printlnGreen("Script compatibility summary", System.err);
      PrintUtils.println("Script path: " + inputPath, System.err);
      if (scriptDesc == null) {
        PrintUtils.printlnRed("Script compatibility is not available. Probably because the script doesn't contain any query", System.err);
      } else {
        scriptDesc.print();
      }

      outputPrintStream.close();
      errorOutputPrintStream.close();
    } else {
      long time = System.currentTimeMillis();
      String outputPath = Paths.get(
          System.getProperty("user.dir"), "result_" + time + ".txt").toString();
      String errorOutputPath = Paths.get(
          System.getProperty("user.dir"), "error_" + time + ".txt").toString();
      String inputDir = cmd.getOptionValue(DIR_OPTION).trim();
      PrintStream outputPrintStream = new PrintStream(new FileOutputStream(new File(outputPath)));
      PrintStream errorOutputPrintStream =
          new PrintStream(new FileOutputStream(new File(errorOutputPath)));

      File dir = new File(inputDir);
      String[] inputPaths = dir.list((d, name) -> name.endsWith(".sql"));

      if (inputPaths == null) {
        throw new IllegalArgumentException("Not a directory: " + inputDir);
      }

      Arrays.sort(inputPaths);

      checkDir(
          dir.getAbsolutePath(),
          inputPaths,
          settings,
          sqlMigrationAssistant,
          ddlDir.toString(),
          outputPath,
          errorOutputPath,
          System.err,
          outputPrintStream,
          errorOutputPrintStream);
    }
  }

  private static Options initOptions() {
    Option queryOption = Option
        .builder(QUERY_OPTION)
        .longOpt(QUERY_LONG_OPTION)
        .argName("Query")
        .hasArg(true)
        .desc("Query to check")
        .build();
    Option fileOption = Option
        .builder(FILE_OPTION)
        .longOpt(FILE_LONG_OPTION)
        .argName("Path")
        .hasArg(true)
        .desc("Script to check")
        .build();
    Option dirOption = Option
        .builder(DIR_OPTION)
        .longOpt(DIR_LONG_OPTION)
        .argName("Path")
        .hasArg(true)
        .desc("Directory that contains SQL scripts")
        .build();
    Option helpOption = Option
        .builder(HELP_OPTION)
        .longOpt(HELP_LONG_OPTION)
        .argName("Help")
        .hasArg(false)
        .desc("Print usage")
        .build();
    Option settingsOption = Option
        .builder(SETTINGS_OPTION)
        .longOpt(SETTINGS_LONG_OPTION)
        .argName("Settings")
        .hasArg(true)
        .desc("Comma-separated MaxCompute SQL settings, like odps.sql.type.system.odps2=true")
        .build();
    return new Options()
        .addOption(queryOption)
        .addOption(fileOption)
        .addOption(helpOption)
        .addOption(settingsOption)
        .addOption(dirOption);
  }

  private static List<String> parseKrbSystemProperties(String krbSystemPropertiesStr) {
    if (StringUtils.isBlank(krbSystemPropertiesStr)) {
      return null;
    }
    return Arrays.asList(krbSystemPropertiesStr.split("\\s*,\\s*"));
  }

  private static Map<String, String> parseSettings(String settingsStr) {
    if (StringUtils.isBlank(settingsStr)) {
      return null;
    }

    List<String> entries = Arrays.asList(settingsStr.split("\\s*,\\s*"));
    Map<String, String> ret = new HashMap<>(entries.size());
    for (String entry : entries) {
      String[] parts = entry.trim().split("\\s*=\\s*");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Illegal setting: " + entry);
      }
      ret.put(parts[0], parts[1]);
    }
    return ret;
  }

  private static CompatibilityDescription check(
      String scriptPath,
      int queryIdx,
      int lastQueryIdx,
      String query,
      Map<String, String> settings,
      SqlMigrationAssistant sqlMigrationAssistant,
      String ddlDir,
      PrintStream progressOutputPrintStream,
      PrintStream outputPrintStream,
      PrintStream errorOutputPrintStream) {
    PrintUtils.printlnYellow(
        "Checking query (" + queryIdx + "/" + lastQueryIdx + ")", progressOutputPrintStream);

    LOG.info("Query: {}, settings: {}", query, GsonUtils.GSON.toJson(settings));

    if (!query.endsWith(";")) {
      query += ";";
    }

    CompatibilityDescription desc = null;
    try {
      try {
        sqlMigrationAssistant.prepareMetadata(query);
        desc = sqlMigrationAssistant.check(query, settings, ddlDir);
      } catch (Exception e) {
        if (e.getCause() instanceof NoSuchObjectException) {
          desc = new CompatibilityDescription() {
            @Override
            public List<Issue> getIssues() {
              Issue issue = new Issue() {
                @Override
                public CompatibilityLevel getCompatibility() {
                  return CompatibilityLevel.ERROR;
                }

                @Override
                public String getDescription() {
                  return "ODPS-0130131:" + e.getCause().getMessage();
                }

                @Override
                public String getSuggestion() {
                  return null;
                }
              };
              return Collections.singletonList(issue);
            }

            @Override
            public String transform() {
              return null;
            }
          };
        } else {
          throw e;
        }
      }
      printResult(
          scriptPath,
          queryIdx,
          query,
          desc,
          outputPrintStream);
    } catch (Exception e) {
      printError(scriptPath, queryIdx, query, e, progressOutputPrintStream);
      if (progressOutputPrintStream != errorOutputPrintStream) {
        printError(scriptPath, queryIdx, query, e, errorOutputPrintStream);
      }
    }
    return desc;
  }

  private static ScriptOrDirCompatibilityDescription checkScript(
      String inputPath,
      Map<String, String> settings,
      SqlMigrationAssistant sqlMigrationAssistant,
      String ddlDir,
      String outputPath,
      String errorOutputPath,
      PrintStream progressOutputPrintStream,
      PrintStream outputPrintStream,
      PrintStream errorOutputPrintStream) throws IOException {

    PrintUtils.printlnYellow("Checking script: " + inputPath, progressOutputPrintStream);
    PrintUtils.printlnYellow("Output path: " + outputPath, progressOutputPrintStream);
    PrintUtils.printlnYellow("Error output path: " + errorOutputPath, progressOutputPrintStream);

    String content = readSqlScript(inputPath);
    String[] queries = content.split("\\s*;\\s*");

    if (queries.length == 0) {
      PrintUtils.printlnRed("No query found", progressOutputPrintStream);
      return null;
    }

    int queryIdx = 1;
    Map<CompatibilityLevel, Integer> compatibilityToQueryCount = new HashMap<>();
    for (String query : queries) {
      if (StringUtils.isBlank(query)) {
        continue;
      }
      CompatibilityDescription desc = check(
          inputPath,
          queryIdx,
          queries.length,
          query,
          settings,
          sqlMigrationAssistant,
          ddlDir,
          System.err,
          outputPrintStream,
          errorOutputPrintStream);
      compatibilityToQueryCount.putIfAbsent(desc.getCompatibility(), 0);
      compatibilityToQueryCount.put(desc.getCompatibility(), compatibilityToQueryCount.get(desc.getCompatibility()) + 1);
      queryIdx += 1;
    }

    return new ScriptOrDirCompatibilityDescription(
        inputPath,
        queries.length,
        compatibilityToQueryCount);
  }

  private static String readSqlScript(String path) throws IOException {
    FileReader reader = new FileReader(path);
    BufferedReader bufferedReader = new BufferedReader(reader);
    StringBuilder sb = new StringBuilder();
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      if (!line.trim().startsWith("--")) {
        sb.append(line).append("\n");
      }
    }

    return sb.toString();
  }

  private static void checkDir(
      String inputDir,
      String[] inputPaths,
      Map<String, String> settings,
      SqlMigrationAssistant sqlMigrationAssistant,
      String ddlDir,
      String outputPath,
      String errorOutputPath,
      PrintStream progressOutputPrintStream,
      PrintStream outputPrintStream,
      PrintStream errorOutputPrintStream) throws IOException {
    PrintUtils.printlnYellow("Input dir: " + inputDir, progressOutputPrintStream);

    List<ScriptOrDirCompatibilityDescription> scriptDescs = new LinkedList<>();
    for (String inputPath : inputPaths) {
      String absoluteInputPath = new File(inputDir, inputPath).getAbsolutePath();
      ScriptOrDirCompatibilityDescription scriptDesc = checkScript(
          absoluteInputPath,
          settings,
          sqlMigrationAssistant,
          ddlDir,
          outputPath,
          errorOutputPath,
          progressOutputPrintStream,
          outputPrintStream,
          errorOutputPrintStream);
      if (scriptDesc != null) {
        scriptDescs.add(scriptDesc);
      }
    }

    for (ScriptOrDirCompatibilityDescription scriptDesc : scriptDescs) {
      PrintUtils.printlnGreen("Script compatibility summary", System.err);
      PrintUtils.println("Script path: " + scriptDesc.getInputPath(), System.err);
      scriptDesc.print();
    }
    PrintUtils.printlnGreen("Directory compatibility summary", progressOutputPrintStream);
    PrintUtils.println("Directory path: " + inputDir, progressOutputPrintStream);
    PrintUtils.println("Number of scripts: " + scriptDescs.size(), progressOutputPrintStream);

    Optional<Integer> totalNumOfQueries = scriptDescs
        .stream()
        .map(ScriptOrDirCompatibilityDescription::getNumOfQueries)
        .reduce((integer, integer2) -> integer + integer2);
    Map<CompatibilityLevel, Integer> compatibilityLevelToQueryCount = new HashMap<>();
    for (ScriptOrDirCompatibilityDescription scriptDesc : scriptDescs) {
      for (Entry<CompatibilityLevel, Integer> entry : scriptDesc.getCompatibilityLevelToQueryCount().entrySet()) {
        compatibilityLevelToQueryCount.putIfAbsent(entry.getKey(), 0);
        compatibilityLevelToQueryCount.put(entry.getKey(), compatibilityLevelToQueryCount.get(entry.getKey()) + entry.getValue());
      }
    }
    ScriptOrDirCompatibilityDescription dirDesc = new ScriptOrDirCompatibilityDescription(
        inputDir,
        totalNumOfQueries.orElse(-1),
        compatibilityLevelToQueryCount);
    dirDesc.print();
  }

  private static void printResult(
      String scriptPath,
      int index,
      String query,
      CompatibilityDescription description,
      PrintStream ps) {
    if (!StringUtils.isBlank(scriptPath)) {
      PrintUtils.println("Script path: " + scriptPath, ps);
    }
    PrintUtils.println("Query index: " + index, ps);
    PrintUtils.println("Query: "  + getTruncatedQuery(query), ps);
    PrintUtils.println("Overall Compatibility Level: " + description.getCompatibility().name(), ps);

    Map<IssueCategory, List<Issue>> issues = new HashMap<>();
    for (Issue issue : description.getIssues()) {
      if (issue.getDescription() != null &&
          issue.getDescription().contains(cases.getString("used.function.desc"))) {
        issues.computeIfAbsent(IssueCategory.UDF, value -> new LinkedList<>()).add(issue);
      } else if (issue.getDescription() != null &&
          issue.getDescription().contains(cases.getString("decimal.usage.desc"))) {
        issues.computeIfAbsent(IssueCategory.DECIMAL, value -> new LinkedList<>()).add(issue);
      } else {
        CompatibilityLevel level = issue.getCompatibility();
        switch (level) {
          case WEEK_WARNINGS:
            issues
                .computeIfAbsent(IssueCategory.WEAK_WARNING, value -> new LinkedList<>())
                .add(issue);
            break;
          case STRONG_WARNINGS:
            issues
                .computeIfAbsent(IssueCategory.STRONG_WARNING, value -> new LinkedList<>())
                .add(issue);
            break;
          case ERROR:
            issues.computeIfAbsent(IssueCategory.ERROR, value -> new LinkedList<>()).add(issue);
            break;
          default:
        }
      }
    }

    PrintUtils.println("Issues: ", ps);
    int issueIdx = 1;
    for (IssueCategory category : IssueCategory.values()) {
      if (issues.containsKey(category)) {
        for (Issue issue : issues.get(category)) {
          PrintUtils.println(String.format("(%d/%d) %s", issueIdx, description.getIssues().size(), category), ps);
          PrintUtils.println("\tCompatibility Level: " + issue.getCompatibility().name(), ps);
          PrintUtils.println("\tDescription: " + issue.getDescription(), ps);
          if (!StringUtils.isBlank(issue.getSuggestion())) {
            PrintUtils.println("\tSuggestion: " + issue.getSuggestion(), ps);
          }
          issueIdx += 1;
        }
      }
    }

    if (!CompatibilityLevel.OK.equals(description.getCompatibility())) {
      if (!query.equalsIgnoreCase(description.transform())) {
        PrintUtils.println("Transformed Query: " + description.transform(), ps);
      } else {
        PrintUtils.println("Transformed Query: N/A" , ps);
      }
    }

    PrintUtils.println("", ps);
  }

  private static void printError(
      String scriptPath,
      int index,
      String query,
      Exception e,
      PrintStream ps) {
    if (!StringUtils.isBlank(scriptPath)) {
      PrintUtils.println("Script path: " + scriptPath, ps);
    }
    PrintUtils.println("Query index: " + index, ps);
    PrintUtils.println("Query: "  + getTruncatedQuery(query), ps);

    PrintUtils.println("Exception: " + ExceptionUtils.getStackTrace(e), ps);
  }

  private static String getTruncatedQuery(String query) {
    if (query.length() > QUERY_TRUNCATE_SIZE) {
      return query.substring(0, QUERY_TRUNCATE_SIZE) + " ...";
    } else {
      return query;
    }
  }
}
