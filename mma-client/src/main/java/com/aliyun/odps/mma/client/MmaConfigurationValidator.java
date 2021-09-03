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

package com.aliyun.odps.mma.client;

import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.DataDestType;
import com.aliyun.odps.mma.config.DataSourceType;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.MetaDestType;
import com.aliyun.odps.mma.config.MetaSourceType;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MmaConfigurationValidator {

  private static final Logger LOG = LogManager.getLogger(MmaConfigurationValidator.class);

  private static final String CONFIG_LONG_OPT = "config";

  public static void main(String args[]) throws Exception {
    Option configOption = Option
        .builder()
        .longOpt(CONFIG_LONG_OPT)
        .hasArg(true)
        .required(true)
        .build();

    Options options = new Options().addOption(configOption);
    DefaultParser defaultParser = new DefaultParser();
    CommandLine cmd = defaultParser.parse(options, args);

    String configPath = cmd.getOptionValue(CONFIG_LONG_OPT);
    String json = IOUtils.toString(Paths.get(configPath).toUri(), StandardCharsets.UTF_8);
    JobConfiguration config = new JobConfiguration(GsonUtils.GSON.fromJson(
        json, new TypeToken<Map<String, String>>() {}.getType()));

    if (config.containsKey(AbstractConfiguration.METADATA_SOURCE_TYPE)) {
      MetaSourceType metaSourceType = MetaSourceType.valueOf(
          config.get(AbstractConfiguration.METADATA_SOURCE_TYPE));
      switch (metaSourceType) {
        case Hive:
          ConfigurationUtils.validateHiveMetaSource(config);
          break;
        case MaxCompute:
          ConfigurationUtils.validateMcMetaSource(config);
          break;
        case OSS:
          ConfigurationUtils.validateOssMetaSource(config);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported value for "
                  + AbstractConfiguration.METADATA_SOURCE_TYPE + ": " + metaSourceType.name());
      }
    }

    if (config.containsKey(AbstractConfiguration.DATA_SOURCE_TYPE)) {
      DataSourceType dataSourceType = DataSourceType.valueOf(
          config.get(AbstractConfiguration.DATA_SOURCE_TYPE));
      switch (dataSourceType) {
        case Hive:
          ConfigurationUtils.validateHiveDataSource(config);
          break;
        case MaxCompute:
          ConfigurationUtils.validateMcDataSource(config);
          break;
        case OSS:
          ConfigurationUtils.validateOssDataSource(config);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported value for "
                  + AbstractConfiguration.DATA_SOURCE_TYPE + ": " + dataSourceType.name());
      }
    }

    if (config.containsKey(AbstractConfiguration.METADATA_DEST_TYPE)) {
      MetaDestType metaDestType = MetaDestType.valueOf(
          config.get(AbstractConfiguration.METADATA_DEST_TYPE));
      switch (metaDestType) {
        case MaxCompute:
          ConfigurationUtils.validateMcMetaDest(config);
          break;
        case OSS:
          ConfigurationUtils.validateOssMetaDest(config);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported value for "
                  + AbstractConfiguration.METADATA_DEST_TYPE + ": " + metaDestType.name());
      }
    }

    if (config.containsKey(AbstractConfiguration.DATA_DEST_TYPE)) {
      DataDestType dataDestType = DataDestType.valueOf(
          config.get(AbstractConfiguration.DATA_DEST_TYPE));
      switch (dataDestType) {
        case MaxCompute:
          ConfigurationUtils.validateMcDataDest(config);
          break;
        case OSS:
          ConfigurationUtils.validateOssDataDest(config);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported value for "
                  + AbstractConfiguration.DATA_DEST_TYPE + ": " + dataDestType.name());
      }
    }
  }
}
