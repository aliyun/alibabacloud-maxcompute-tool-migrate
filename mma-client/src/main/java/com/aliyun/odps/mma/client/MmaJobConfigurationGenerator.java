package com.aliyun.odps.mma.client;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.util.GsonUtils;
import com.csvreader.CsvReader;

public class MmaJobConfigurationGenerator {

  private static final Logger LOG = LogManager.getLogger(MmaJobConfigurationGenerator.class);

  /**
   *  Acceptable formats:
   *    source_db.source_tbl:dest_db.dest_tbl
   *    source_db.source_tbl("pt_begin", "pt_end", "order_type1/order_type2/...") dest_db.dest_tbl
   *    pt_begin = pt_end = pt_v1,pt_v2,... (pt_begin <= pt_end)
   *    order_type = num or lex
   */
  private static final Pattern TABLE_MAPPING_LINE_PATTERN =
      Pattern.compile("([^()]+)\\.([^()]+)(\\([^()]+\\))?:([^()]+)\\.([^()]+)");

  private static final String HELP_OPT = "h";
  private static final String HELP_LONG_OPT = "help";
  private static final String OBJECT_TYPE_LONG_OPT = "objecttype";
  private static final String TABLE_MAPPING_PATH_LONG_OPT = "tablemapping";
  private static final String SOURCE_CATALOG_LONG_OPT = "sourcecatalog";
  private static final String DEST_CATALOG_LONG_OPT = "destcatalog";
  private static final String JOB_ID_LONG_OPT = "jobid";
  private static final String OUTPUT_PATH_OPT = "output";

  private static String mmaHome;

  private static Options initOptions() {
    Option helpOption = Option
        .builder(HELP_OPT)
        .longOpt(HELP_LONG_OPT)
        .hasArg(false)
        .desc("Print usage")
        .build();
    Option objectTypeOption = Option
        .builder()
        .longOpt(OBJECT_TYPE_LONG_OPT)
        .hasArg()
        .required(false)
        .desc("Required, object type")
        .build();
    Option tableMappingPathOption = Option
        .builder()
        .longOpt(TABLE_MAPPING_PATH_LONG_OPT)
        .hasArg()
        .desc("Required when object type is TABLE, path to the table_mapping.txt")
        .required(false)
        .build();
    Option sourceCatalogOption = Option
        .builder()
        .longOpt(SOURCE_CATALOG_LONG_OPT)
        .hasArg()
        .desc("Required when the object type is CATALOG, source CATALOG name")
        .required(false)
        .build();
    Option destCatalogOption = Option
        .builder()
        .longOpt(DEST_CATALOG_LONG_OPT)
        .hasArg()
        .desc("Required when the object type is CATALOG, destination CATALOG name")
        .required(false)
        .build();
    Option jobIdOption = Option
        .builder()
        .longOpt(JOB_ID_LONG_OPT)
        .hasArg()
        .desc("The job ID")
        .build();
    Option outputPathOption = Option
        .builder()
        .longOpt(OUTPUT_PATH_OPT)
        .hasArg()
        .desc("The output directory path")
        .build();
    return new Options()
        .addOption(helpOption)
        .addOption(objectTypeOption)
        .addOption(tableMappingPathOption)
        .addOption(sourceCatalogOption)
        .addOption(destCatalogOption)
        .addOption(jobIdOption)
        .addOption(outputPathOption);
  }

  private static int help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String syntax = "gen-job-conf --config /path/to/mma_server_config.json --objecttype [CATALOG | TABLE] [options]";
    formatter.printHelp(syntax, options);
    return 0;
  }

  private static void writeToFile(
      CommandLine cmd,
      String jobId,
      ObjectType objectType,
      String sourceObjectFullName,
      String destObjectFullName,
      String json) throws IOException {
    long time = System.currentTimeMillis();
    String fileName = String.format(
        "%s-%s-%s-%s.json",
        objectType.name(),
        sourceObjectFullName,
        destObjectFullName,
        StringUtils.defaultIfBlank(jobId, Long.toString(time)));
    Path path;
    if (cmd.hasOption(OUTPUT_PATH_OPT)) {
      path = Paths.get(cmd.getOptionValue(OUTPUT_PATH_OPT), fileName);
    } else {
      path = Paths.get(mmaHome, "conf", fileName);
    }
    FileUtils.write(path.toFile(), json, StandardCharsets.UTF_8);
    System.out.println("Job configuration generated: " + path.toString());
  }

  private static int generateCatalogJobConfiguration(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(SOURCE_CATALOG_LONG_OPT) || !cmd.hasOption(DEST_CATALOG_LONG_OPT)) {
      throw new IllegalArgumentException(String.format(
          "Missing required option %s or %s", SOURCE_CATALOG_LONG_OPT, DEST_CATALOG_LONG_OPT));
    }
    String sourceCatalog = cmd.getOptionValue(SOURCE_CATALOG_LONG_OPT);
    String destCatalog = cmd.getOptionValue(DEST_CATALOG_LONG_OPT);
    Map<String, String> builder = new HashMap<>();
    builder.put(JobConfiguration.OBJECT_TYPE, ObjectType.CATALOG.name());
    builder.put(JobConfiguration.SOURCE_CATALOG_NAME, sourceCatalog);
    builder.put(JobConfiguration.DEST_CATALOG_NAME, destCatalog);
    String jobId = null;
    if (cmd.hasOption(JOB_ID_LONG_OPT)) {
      jobId = cmd.getOptionValue(JOB_ID_LONG_OPT);
      builder.put(JobConfiguration.JOB_ID, jobId);
    }
    String json = GsonUtils.GSON.toJson(builder);
    writeToFile(cmd, jobId, ObjectType.CATALOG, sourceCatalog, destCatalog, json);
    return 0;
  }

  private static int generateTableJobConfiguration(CommandLine cmd) throws IOException {
    if (!cmd.hasOption(TABLE_MAPPING_PATH_LONG_OPT)) {
      throw new IllegalArgumentException("Missing required option " + TABLE_MAPPING_PATH_LONG_OPT);
    }
    String tableMappingFilePath = cmd.getOptionValue(TABLE_MAPPING_PATH_LONG_OPT);
    List<String> lines =
        FileUtils.readLines(new File(tableMappingFilePath), StandardCharsets.UTF_8);
    for (String line : lines) {
      if (StringUtils.isBlank(line) || line.trim().startsWith("#")) {
        continue;
      }

      Matcher matcher = TABLE_MAPPING_LINE_PATTERN.matcher(line);
      if (matcher.matches()) {
        if (matcher.groupCount() != 5) {
          System.err.println("[ERROR] Invalid line: " + line);
          continue;
        }

        String sourceCatalog = Validate.notBlank(
            matcher.group(1),
            "Source catalog name cannot be null or empty");
        String sourceTbl = Validate.notBlank(
            matcher.group(2),
            "Source table name cannot be null or empty");
        String destCatalog = Validate.notBlank(
            matcher.group(4),
            "Destination catalog name cannot be null or empty");
        String destTbl = Validate.notBlank(
            matcher.group(5),
            "Destination table cannot be null or empty");

        List<String> partitionValues = null;

        if (matcher.group(3) != null) {
          // Remove parentheses
          String partitionValuesStr = matcher.group(3).substring(1, matcher.group(3).length() - 1);
          CsvReader csvReader = new CsvReader(new StringReader(partitionValuesStr));
          if (csvReader.readRecord()) {
            partitionValues = Arrays.asList(csvReader.getValues());
          } else {
            System.err.println("[ERROR] Invalid partition values: " + matcher.group(3));
            continue;
          }
        }
        Map<String, String> builder = new HashMap<>();
        builder.put(JobConfiguration.OBJECT_TYPE, ObjectType.TABLE.name());
        builder.put(JobConfiguration.SOURCE_CATALOG_NAME, sourceCatalog);
        builder.put(JobConfiguration.SOURCE_OBJECT_NAME, sourceTbl);
        builder.put(JobConfiguration.DEST_CATALOG_NAME, destCatalog);
        builder.put(JobConfiguration.DEST_OBJECT_NAME, destTbl);
        if (partitionValues != null) {
          builder.put(JobConfiguration.PARTITION_BEGIN, partitionValues.get(0));
          builder.put(JobConfiguration.PARTITION_END, partitionValues.get(1));
          builder.put(JobConfiguration.PARTITION_ORDER, partitionValues.get(2));
        }
        String json = GsonUtils.GSON.toJson(builder);
        writeToFile(
            cmd,
            null,
            ObjectType.TABLE,
            sourceCatalog + "." + sourceTbl,
            destCatalog + "." + destTbl,
            json);
      } else {
        System.err.println("[WARN] Invalid line: " + line);
      }
    }
    return 0;
  }

  public static void main(String[] args) throws ParseException, IOException {
    mmaHome = System.getenv("MMA_HOME");
    if (mmaHome == null) {
      throw new IllegalStateException("Environment variable 'MMA_HOME' is missing");
    }

    Options options = initOptions();
    DefaultParser defaultParser = new DefaultParser();
    CommandLine cmd = defaultParser.parse(options, args);

    if (cmd.hasOption(HELP_OPT)) {
      System.exit(help(options));
    }

    if (!cmd.hasOption(OBJECT_TYPE_LONG_OPT)) {
      throw new IllegalArgumentException("Missing required option " + OBJECT_TYPE_LONG_OPT);
    }
    ObjectType objectType =
        ObjectType.valueOf(cmd.getOptionValue(OBJECT_TYPE_LONG_OPT).toUpperCase());
    int returnCode;
    switch (objectType) {
      case CATALOG:
        returnCode = generateCatalogJobConfiguration(cmd);
        break;
      case TABLE:
        returnCode = generateTableJobConfiguration(cmd);
        break;
      default:
        throw new IllegalArgumentException("Unsupported object type: " + objectType);
    }

    System.exit(returnCode);
  }
}
