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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.BasicConfigurator;

import com.aliyun.odps.datacarrier.taskscheduler.MmaEventConfig.MmaEventSenderConfig;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventManager;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventSenderFactory;
import com.aliyun.odps.datacarrier.taskscheduler.event.MmaEventType;
import com.aliyun.odps.datacarrier.taskscheduler.resource.Resource;
import com.aliyun.odps.datacarrier.taskscheduler.resource.ResourceAllocator;


public class MmaServerMain {
  /*
    Options
   */
  private static final String CONFIG_OPT = "config";
  private static final String HELP_OPT = "help";

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

  public static void main(String[] args)
      throws ParseException, IOException, MetaException, MmaException {
    BasicConfigurator.configure();

    String mmaHome = System.getenv("MMA_HOME");
    if (mmaHome == null) {
      throw new IllegalStateException("Environment variable 'MMA_HOME' not set");
    }

    Option configOption = Option
        .builder(CONFIG_OPT)
        .longOpt(CONFIG_OPT)
        .argName(CONFIG_OPT)
        .hasArg()
        .desc("MMA server configuration path")
        .build();

    /*
      Help
     */
    Option helpOption = Option
        .builder("h")
        .longOpt(HELP_OPT)
        .argName(HELP_OPT)
        .hasArg(false)
        .desc("Print usage")
        .build();

    Options options = new Options()
        .addOption(configOption)
        .addOption(helpOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption(HELP_OPT)) {
      System.exit(help(options));
    }

    Path mmaServerConfigPath;
    if (!cmd.hasOption(CONFIG_OPT)) {
      mmaServerConfigPath = Paths.get(mmaHome, "conf", "mma_server_config.json");
    } else {
      mmaServerConfigPath = Paths.get(cmd.getOptionValue("config"));
    }

    // Setup MmaServerConfig singleton
    MmaServerConfig.init(mmaServerConfigPath);

    // Setup MmaEventManager singleton
    if (MmaServerConfig.getInstance().getEventConfig() != null) {
      for (MmaEventSenderConfig eventSenderConfig : MmaServerConfig.getInstance()
                                                                   .getEventConfig()
                                                                   .getEventSenderConfigs()) {
        MmaEventManager.getInstance().register(MmaEventSenderFactory.get(eventSenderConfig));
      }

      List<MmaEventType> whitelist = MmaServerConfig.getInstance().getEventConfig().getWhitelist();
      List<MmaEventType> blacklist = MmaServerConfig.getInstance().getEventConfig().getBlacklist();
      if (whitelist != null) {
        whitelist.forEach(eventType -> MmaEventManager.getInstance().whitelist(eventType));
      }
      if (blacklist != null) {
        blacklist.forEach(eventType -> MmaEventManager.getInstance().blacklist(eventType));
      }
    }

    // Setup MmaResourceAllocator singleton
    Map<String, String> resourceConfig = MmaServerConfig.getInstance().getResourceConfig();
    for (Entry<String, String> entry : resourceConfig.entrySet()) {
      Resource resource = Resource.valueOf(entry.getKey().trim().toUpperCase());
      Integer number = Integer.valueOf(entry.getValue());
      ResourceAllocator.getInstance().update(resource, number);
    }

    MmaServer mmaServer = new MmaServer();
    try {
      mmaServer.run();
    } finally {
      mmaServer.shutdown();
    }

    MmaEventManager.getInstance().shutdown();
  }
}
