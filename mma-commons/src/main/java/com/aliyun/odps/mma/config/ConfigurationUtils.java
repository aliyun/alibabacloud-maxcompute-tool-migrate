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

package com.aliyun.odps.mma.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.meta.MetaSourceFactory;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.odps.mma.exception.MmaException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

public class ConfigurationUtils {

  public static void validateMc(
      String endpoint,
      String project,
      String accessKeyId,
      String accessKeySecret) throws MmaException {

    AliyunAccount aliyunAccount = new AliyunAccount(accessKeyId, accessKeySecret);
    Odps odps = new Odps(aliyunAccount);
    odps.setEndpoint(endpoint);

    try {
      odps.projects().get(project).reload();
    } catch (OdpsException e) {
      throw new MmaException("Invalid MaxCompute configuration", e);
    }
  }

  public static void validateOss(
      String endpoint,
      String bucket,
      String accessKeyId,
      String accessKeySecret) throws MmaException {

    OSS oss = (new OSSClientBuilder()).build(endpoint, accessKeyId, accessKeySecret);
    if (oss.listBuckets().stream().noneMatch(b -> bucket.equals(b.getName()))) {
      throw new MmaException("Invalid OSS configuration");
    }
  }

  public static void validateHiveMetastore(JobConfiguration config) throws MetaException {
    MetaSourceFactory.getHiveMetaSource(config);
  }

  public static void validateHiveJdbc(
      String hiveJdbcUrl,
      String username,
      String password) throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    try (Connection conn = DriverManager.getConnection(hiveJdbcUrl, username, password)) {
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT current_database()")) {
          if (rs.next() && !"default".equalsIgnoreCase(rs.getString(1))) {
            throw new IllegalArgumentException(
                "Invalid Hive JDBC connection URL, please use the default database");
          }
        }
      }
    }
  }

  public static String toPartitionIdentifier(
      String tableName,
      List<String> partitionValues) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("TableName", tableName);
    JsonArray jsonArray = new JsonArray();
    partitionValues.forEach(pv -> jsonArray.add(new JsonPrimitive(pv)));
    jsonObject.add("PartitionValues", jsonArray);
    return jsonObject.toString();
  }

  public static String getTableNameFromPartitionIdentifier(String partitionIdentifier) {
    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(partitionIdentifier).getAsJsonObject();
    return jsonObject.get("TableName").getAsString();
  }

  public static List<String> getPartitionValuesFromPartitionIdentifier(String partitionIdentifier) {
    JsonParser parser = new JsonParser();
    JsonObject jsonObject = parser.parse(partitionIdentifier).getAsJsonObject();
    JsonArray jsonArray = jsonObject.get("PartitionValues").getAsJsonArray();
    Iterator<JsonElement> iter = jsonArray.iterator();

    List<String> ret = new ArrayList<>();
    while (iter.hasNext()) {
      JsonElement jsonElement = iter.next();
      ret.add(jsonElement.getAsString());
    }

    return ret;
  }
}
