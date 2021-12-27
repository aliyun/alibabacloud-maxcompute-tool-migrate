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

package com.aliyun.odps.mma.server;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.server.config.MmaEventSenderConfiguration;
import com.aliyun.odps.mma.server.config.MmaServerConfiguration;
import com.aliyun.odps.mma.server.event.MmaEventManager;
import com.aliyun.odps.mma.server.event.MmaEventSenderFactory;
import com.aliyun.odps.mma.server.event.MmaEventType;
import com.aliyun.odps.mma.server.resource.MmaResourceManager;
import com.aliyun.odps.mma.server.resource.Resource;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MmaServerMain {
  private static final Logger LOG = LogManager.getLogger(MmaServerMain.class);

  private static final String CONFIG_OPT = "c";
  private static final String CONFIG_LONG_OPT = "config";
  private static final String HELP_OPT = "h";
  private static final String HELP_LONG_OPT = "help";

  /**
   * Print help info.
   *
   * @return Always return 0.
   */
  private static int help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String cmdLineSyntax = "mma-server";
    formatter.printHelp(cmdLineSyntax, options);
    return 0;
  }

  public static void main(String[] args) throws Exception {

    String mmaHome = System.getenv("MMA_HOME");
    if (mmaHome == null) {
      throw new IllegalStateException("Environment variable 'MMA_HOME' is missing");
    }

    Option configOption = Option
        .builder(CONFIG_OPT)
        .longOpt(CONFIG_LONG_OPT)
        .hasArg(true)
        .argName("MMA server conf path")
        .desc("Default value: ${MMA_HOME}/conf/mma_server_config.json")
        .build();
    Option helpOption = Option
        .builder(HELP_OPT)
        .longOpt(HELP_LONG_OPT)
        .hasArg(false)
        .desc("Print usage")
        .build();

    Options options = new Options().addOption(configOption).addOption(helpOption);
    DefaultParser defaultParser = new DefaultParser();
    CommandLine cmd = defaultParser.parse(options, args);

    if (cmd.hasOption(HELP_OPT)) {
      System.exit(help(options));
    }

    Path mmaServerConfigPath;
    if (!cmd.hasOption(CONFIG_OPT)) {
      mmaServerConfigPath = Paths.get(mmaHome, "conf", "mma_server_config.json");
    } else {
      mmaServerConfigPath = Paths.get(cmd.getOptionValue(CONFIG_OPT));
    }

    initMmaServerConfigurationSingleton(mmaServerConfigPath);
    initMmaEventManagerSingleton();
    initMmaResourceManagerSingleton();

    try {
      MmaServer mmaServer = new MmaServer();
      mmaServer.run();
    } catch (Exception e) {
      LOG.error("Failed to start MMA server", e);
    } finally {
      MmaEventManager.getInstance().shutdown();
    }
  }

  private static void initMmaServerConfigurationSingleton(Path path) throws IOException {
    String json = IOUtils.toString(path.toUri(), StandardCharsets.UTF_8);
    Map<String, String> map = GsonUtils.GSON.fromJson(
        json, new TypeToken<Map<String, String>>() {}.getType());
    MmaServerConfiguration.setInstance(map);
  }

  private static void initMmaEventManagerSingleton() {
    boolean eventEnabled = Boolean.valueOf(
        MmaServerConfiguration.getInstance().getOrDefault(
            MmaServerConfiguration.EVENT_ENABLED,
            MmaServerConfiguration.EVENT_ENABLED_DEFAULT_VALUE)
    );
    if (eventEnabled) {
      String eventSenders =
          MmaServerConfiguration.getInstance().get(MmaServerConfiguration.EVENT_SENDERS);
      if (!StringUtils.isBlank(eventSenders)) {
        List<MmaEventSenderConfiguration> eventSenderConfigs = GsonUtils.GSON.fromJson(
            eventSenders, new TypeToken<List<MmaEventSenderConfiguration>>() {}.getType());
        for (MmaEventSenderConfiguration eventSenderConfig : eventSenderConfigs) {
          MmaEventManager.getInstance().register(MmaEventSenderFactory.get(eventSenderConfig));
        }
      }
      String eventTypes = MmaServerConfiguration.getInstance().getOrDefault(
          MmaServerConfiguration.EVENT_TYPES, MmaServerConfiguration.EVENT_TYPES_DEFAULT_VALUE);
      for (String eventType : eventTypes.split("\\s*,\\s*")) {
        MmaEventManager.getInstance().whitelist(MmaEventType.valueOf(eventType));
      }
    }
  }

  private static void initMmaResourceManagerSingleton() {
    // All resources should be initialized with their default value
    long numDataWorker = Long.valueOf(MmaServerConfiguration.getInstance().getOrDefault(
        MmaServerConfiguration.RESOURCE_DATA_WORKER,
        MmaServerConfiguration.RESOURCE_DATA_WORKER_DEFAULT_VALUE));
    MmaResourceManager.getInstance().update(Resource.DATA_WORKER, numDataWorker);
    long numMetadataWorker = Long.valueOf(MmaServerConfiguration.getInstance().getOrDefault(
        MmaServerConfiguration.RESOURCE_METADATA_WORKER,
        MmaServerConfiguration.RESOURCE_METADATA_WORKER_DEFAULT_VALUE));
    MmaResourceManager.getInstance().update(Resource.METADATA_WORKER, numMetadataWorker);
  }
}
