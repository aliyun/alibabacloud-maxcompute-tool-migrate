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
 *
 */

package com.aliyun.odps.mma.io;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.mapred.JobConf;

public class OdpsConfig {

  private static final String ACCESS_ID = "access_id";
  private static final String ACCESS_KEY = "access_key";

  private Properties properties;

  public OdpsConfig(MapredContext context, String filename) throws IOException {
    Path path = new Path(filename);
    FileSystem fs;
    if (context != null) {
      System.out.println("MMA init fs with job conf");
      fs = FileSystem.get(context.getJobConf());
    } else {
      System.out.println("MMA init fs with new configuration");
      fs = path.getFileSystem(new Configuration());
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    this.properties = new Properties();
    this.properties.load(br);
  }

  public String getAccessId() {
    return this.properties.getProperty(ACCESS_ID);
  }

  public String getAccessKey() {
    return this.properties.getProperty(ACCESS_KEY);
  }

}