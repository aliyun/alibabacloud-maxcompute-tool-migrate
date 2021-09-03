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
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.JobInfoOutputsV1;
import com.aliyun.odps.mma.config.ObjectType;

public class MmaClientMain {

  private static final String SUBMIT_JOB_ACTION = "SUBMITJOB";
  private static final String LIST_JOBS_ACTION = "LISTJOBS";
  private static final String GET_JOB_INFO_ACTION = "GETJOBINFO";
  private static final String STOP_JOB_ACTION = "STOPJOB";
  private static final String DELETE_JOB_ACTION = "DELETEJOB";
  private static final String RESET_JOB_ACTION = "RESETJOB";

  private static final String HOST_LONG_OPT = "host";
  private static final String PORT_LONG_OPT = "port";
  private static final String ACTION_OPT = "a";
  private static final String ACTION_LONG_OPT = "action";
  private static final String HELP_OPT = "h";
  private static final String HELP_LONG_OPT = "help";
  private static final String CONFIG_OPT = "c";
  private static final String CONFIG_LONG_OPT = "config";
  private static final String JOB_ID_OPT = "jid";
  private static final String JOB_ID_LONG_OPT = "jobid";

  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 18889;

  private static Options initOptions() {
    Option hostOption = Option
        .builder()
        .longOpt(HOST_LONG_OPT)
        .argName("Hostname")
        .hasArg(true)
        .desc("Hostname of MMA server")
        .build();
    Option portOption = Option
        .builder()
        .longOpt(PORT_LONG_OPT)
        .argName("Port")
        .hasArg(true)
        .desc("Port of MMA server")
        .build();
    Option actionOption = Option
        .builder(ACTION_OPT)
        .longOpt(ACTION_LONG_OPT)
        .argName("Action")
        .hasArg(true)
        .desc(String.format(
            "Could be '%s', '%s', '%s', '%s', '%s', and '%s'",
            SUBMIT_JOB_ACTION,
            RESET_JOB_ACTION,
            LIST_JOBS_ACTION,
            GET_JOB_INFO_ACTION,
            STOP_JOB_ACTION,
            DELETE_JOB_ACTION))
        .build();
    Option helpOption = Option
        .builder(HELP_OPT)
        .longOpt(HELP_LONG_OPT)
        .hasArg(false)
        .desc("Print usage")
        .build();
    Option configOption = Option
        .builder(CONFIG_OPT)
        .longOpt(CONFIG_LONG_OPT)
        .argName("Job conf path")
        .desc("Required by action 'Submit'")
        .hasArg(true)
        .build();
    Option jobIdOption = Option
        .builder(JOB_ID_OPT)
        .longOpt(JOB_ID_LONG_OPT)
        .argName("Job ID")
        .desc(String.format(
            "Required by action '%s', '%s', '%s', and '%s'",
            GET_JOB_INFO_ACTION,
            RESET_JOB_ACTION,
            STOP_JOB_ACTION,
            DELETE_JOB_ACTION))
        .hasArg(true)
        .build();
    return new Options()
        .addOption(hostOption)
        .addOption(portOption)
        .addOption(actionOption)
        .addOption(helpOption)
        .addOption(configOption)
        .addOption(jobIdOption);
  }

  private static int help(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    String syntax = String.format(
        "mma-client --action [%s | %s | %s | %s | %s | %s] [options]",
        SUBMIT_JOB_ACTION,
        RESET_JOB_ACTION,
        LIST_JOBS_ACTION,
        GET_JOB_INFO_ACTION,
        STOP_JOB_ACTION,
        DELETE_JOB_ACTION);
    formatter.printHelp(syntax, options);
    return 0;
  }

  private static int submitJob(MmaClient mmaClient, CommandLine cmd) throws Exception {
    if (!cmd.hasOption(CONFIG_OPT)) {
      throw new IllegalArgumentException("Missing required option " + CONFIG_OPT);
    }
    String jobConfigPath = cmd.getOptionValue(CONFIG_OPT);
    String content = IOUtils.toString(
        new FileInputStream(jobConfigPath), StandardCharsets.UTF_8);
    JobConfiguration config = JobConfiguration.fromJson(content);
    System.err.println("Submitting job, this may take a few minutes");
    mmaClient.submitJob(config);
    System.err.println("OK");
    return 0;
  }

  private static int resetJob(MmaClient mmaClient, CommandLine cmd) throws Exception {
    if (!cmd.hasOption(JOB_ID_OPT)) {
      throw new IllegalArgumentException("Missing required option " + JOB_ID_LONG_OPT);
    }
    String jobId = cmd.getOptionValue(JOB_ID_OPT);
    System.err.println("Resetting job, this may take a few minutes");
    mmaClient.resetJob(jobId);
    System.err.println("OK");
    return 0;
  }

  private static int listJobs(MmaClient mmaClient) throws Exception {
    List<JobInfoOutputsV1> jobInfos = mmaClient.listJobs();
    for (JobInfoOutputsV1 jobInfo : jobInfos) {
      String line = String.format(
          "Job ID: %s, status: %s, progress: %.2f%%",
          jobInfo.getJobId(),
          jobInfo.getStatus(),
          jobInfo.getProgress());
      System.out.println(line);
    }
    System.err.println("OK");
    return 0;
  }

  private static int getJobInfo(MmaClient mmaClient, CommandLine cmd) throws Exception {
    if (!cmd.hasOption(JOB_ID_OPT)) {
      throw new IllegalArgumentException("Missing required option " + JOB_ID_LONG_OPT);
    }
    String jobId = cmd.getOptionValue(JOB_ID_OPT);
    JobInfoOutputsV1 jobInfo = mmaClient.getJobInfo(jobId);
    StringBuilder sb = new StringBuilder();
    sb.append("Job ID: ").append(jobInfo.getJobId()).append("\n");
    sb.append("Job status: ").append(jobInfo.getStatus()).append("\n");
    sb.append("Object type: ").append(jobInfo.getObjectType()).append("\n");
    sb.append("Source: ")
      .append(jobInfo.getSourceCatalog());
    if (!ObjectType.CATALOG.name().equals(jobInfo.getObjectType())) {
      sb.append(".")
        .append(jobInfo.getSourceObject());
    }
    sb.append("\n");
    sb.append("Destination: ")
      .append(jobInfo.getDestCatalog());
    if (!ObjectType.CATALOG.name().equals(jobInfo.getObjectType())) {
      sb.append(".")
        .append(jobInfo.getDestObject());
    }
    System.out.println(sb.toString());
    System.err.println("OK");
    return 0;
  }

  private static int stopJob(MmaClient mmaClient, CommandLine cmd) throws Exception {
    if (!cmd.hasOption(JOB_ID_OPT)) {
      throw new IllegalArgumentException("Missing required option " + JOB_ID_LONG_OPT);
    }
    String jobId = cmd.getOptionValue(JOB_ID_OPT);
    mmaClient.stopJob(jobId);
    System.err.println("OK");
    return 0;
  }

  private static int deleteJob(MmaClient mmaClient, CommandLine cmd) throws Exception {
    if (!cmd.hasOption(JOB_ID_OPT)) {
      throw new IllegalArgumentException("Missing required option " + JOB_ID_LONG_OPT);
    }
    String jobId = cmd.getOptionValue(JOB_ID_OPT);
    mmaClient.deleteJob(jobId);
    System.err.println("OK");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Options options = initOptions();
    DefaultParser defaultParser = new DefaultParser();
    CommandLine cmd = defaultParser.parse(options, args);

    if (cmd.hasOption(HELP_OPT)) {
      System.exit(help(options));
    }

    if (!cmd.hasOption(ACTION_OPT)) {
      throw new IllegalArgumentException("Missing required option " + ACTION_LONG_OPT);
    }
    String host = cmd.hasOption(HOST_LONG_OPT) ? cmd.getOptionValue(HOST_LONG_OPT) : DEFAULT_HOST;
    int port = cmd.hasOption(PORT_LONG_OPT) ?
        Integer.valueOf(cmd.getOptionValue(PORT_LONG_OPT)) : DEFAULT_PORT;
    MmaClient mmaClient = new MmaClient(host, port);
    String action = cmd.getOptionValue(ACTION_OPT).toUpperCase();
    int returnCode;
    switch (action) {
      case SUBMIT_JOB_ACTION:
        returnCode = submitJob(mmaClient, cmd);
        break;
      case LIST_JOBS_ACTION:
        returnCode = listJobs(mmaClient);
        break;
      case GET_JOB_INFO_ACTION:
        returnCode = getJobInfo(mmaClient, cmd);
        break;
      case STOP_JOB_ACTION:
        returnCode = stopJob(mmaClient, cmd);
        break;
      case DELETE_JOB_ACTION:
        returnCode = deleteJob(mmaClient, cmd);
        break;
      case RESET_JOB_ACTION:
        returnCode = resetJob(mmaClient, cmd);
        break;
      default:
        throw new IllegalArgumentException("Unsupported action: " + action);
    }

    System.exit(returnCode);
  }
}
