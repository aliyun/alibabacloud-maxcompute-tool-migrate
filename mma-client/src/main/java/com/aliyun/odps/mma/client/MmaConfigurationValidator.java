package com.aliyun.odps.mma.client;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.commons.util.IOUtils;
import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MmaConfigurationValidator {

  private static final Logger LOG = LogManager.getLogger(MmaConfigurationValidator.class);

  private static final String IS_SOURCE_LONG_OPT = "source";
  private static final String TYPE_LONG_OPT = "type";
  private static final String CONFIG_LONG_OPT = "config";

  public static void main(String args[]) throws Exception {
    Option isSourceOption = Option
        .builder()
        .longOpt(IS_SOURCE_LONG_OPT)
        .hasArg(false)
        .build();
    Option configOption = Option
        .builder()
        .longOpt(CONFIG_LONG_OPT)
        .hasArg(true)
        .required(true)
        .build();
    Option typeOption = Option
        .builder()
        .longOpt(TYPE_LONG_OPT)
        .hasArg(true)
        .required(true)
        .desc("Could be MaxCompute, Hive, and OSS")
        .build();

    Options options = new Options()
        .addOption(configOption)
        .addOption(typeOption)
        .addOption(isSourceOption);
    DefaultParser defaultParser = new DefaultParser();
    CommandLine cmd = defaultParser.parse(options, args);

    String type = cmd.getOptionValue(TYPE_LONG_OPT);
    String configPath = cmd.getOptionValue(CONFIG_LONG_OPT);
    boolean isSource = cmd.hasOption(IS_SOURCE_LONG_OPT);
    LOG.info("Type: {}, config path: {}, is source: {}", type, configPath, isSource);

    String json = new String(
        IOUtils.readFully(new FileInputStream(configPath)),
        StandardCharsets.UTF_8);
    Map<String, String> config = GsonUtils.GSON.fromJson(
        json, new TypeToken<Map<String, String>>() {}.getType());

    switch (type) {
      case "Hive": {
        if (!isSource) {
          throw new IllegalArgumentException("Unsupported destination: Hive");
        }
        String hiveMetastoreUris =
            config.get(AbstractConfiguration.METADATA_SOURCE_HIVE_METASTORE_URIS);
        Validate.notNull(hiveMetastoreUris, "Hive metastore URI(s) cannot be null or empty");
        ConfigurationUtils.validateHiveMetastore(new JobConfiguration(config));
        String hiveJdbcUrl = config.get(AbstractConfiguration.DATA_SOURCE_HIVE_JDBC_URL);
        Validate.notNull(hiveJdbcUrl, "Hive JDBC connection URL cannot be null or empty");
        String username = config.get(AbstractConfiguration.DATA_SOURCE_HIVE_JDBC_USERNAME);
        Validate.notNull(username, "Hive JDBC username cannot be null or empty");
        String password = config.get(AbstractConfiguration.DATA_SOURCE_HIVE_JDBC_PASSWORD);
        Validate.notNull(password, "Hive JDBC password cannot be null or empty");
        ConfigurationUtils.validateHiveJdbc(hiveJdbcUrl, username, password);
        break;
      }
      case "MaxCompute": {
        if (isSource) {
          // TODO:
          throw new IllegalArgumentException("Unsupported source: MaxCompute");
        }

        String endpoint = config.get(AbstractConfiguration.DATA_DEST_MC_ENDPOINT);
        Validate.notNull(endpoint, "MC endpoint cannot be null or empty");
        String accessKeyId = config.get(AbstractConfiguration.DATA_DEST_MC_ACCESS_KEY_ID);
        Validate.notNull(accessKeyId, "MC accesskey ID cannot be null or empty");
        String accessKeySecret = config.get(AbstractConfiguration.DATA_DEST_MC_ACCESS_KEY_SECRET);
        Validate.notNull(accessKeySecret, "MC accesskey secret cannot be null or empty");
        String project = config.get(AbstractConfiguration.JOB_EXECUTION_MC_PROJECT);
        Validate.notNull(project, "MC default project cannot be null or empty");
        ConfigurationUtils.validateMc(endpoint, project, accessKeyId, accessKeySecret);
      }
      break;
      case "OSS": {
        // TODO
        throw new IllegalStateException("Unsupported type: OSS");
      }
      default:
        throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
}
