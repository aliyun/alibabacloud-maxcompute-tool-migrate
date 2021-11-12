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

package com.aliyun.odps.mma.io;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.BearerTokenAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.mma.io.converter.HiveObjectConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.UploadSession;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import com.aliyun.odps.type.TypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

public class McDataTransmissionUDTF extends GenericUDTF {
  /**
   * Won't change once initialized
   */
  Odps odps;
  TableTunnel tunnel;
  ObjectInspector[] objectInspectors;
  private String odpsProjectName;
  private String odpsTableName;
  private List<String> odpsColumnNames;
  private List<String> odpsPartitionColumnNames;
  private TableSchema schema;

  /**
   * Changes with different partition
   */
  private Map<String, UploadSession> partitionSpecToUploadSession = new HashMap<>();
  private UploadSession currentUploadSession;
  private RecordWriter recordWriter;
  private String currentOdpsPartitionSpec;

  /**
   * Reused objects
   */
  private Object[] hiveColumnValues;
  private Object[] hivePartitionColumnValues;
  private Record reusedRecord;

  /**
   * Metrics
   */
  private long startTime = System.currentTimeMillis();
  private long bytesTransferred = 0L;
  private Long numRecordTransferred = 0L;
  private Object[] forwardObj = new Object[1];

  private static final int IDX_AUTH_TYPE = 0;
  private static final int IDX_ODPS_CONFIG_PATH = 1;
  private static final int IDX_TOKEN = 1;
  private static final int IDX_ENDPOINT = 2;
  private static final int IDX_TUNNEL_ENDPOINT = 3;
  private static final int IDX_PROJECT = 4;
  private static final int IDX_TABLE = 5;
  private static final int IDX_COLUMNS = 6;
  private static final int IDX_PTS = 7;
  private static final int IDX_COL_START = 8;
  private static int IDX_PT_BEGIN = IDX_COL_START;

  private MapredContext mapredContext;


  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    objectInspectors = args;
    List<String> fieldNames = new ArrayList<>();
    fieldNames.add("num_record_transferred");
    List<ObjectInspector> outputObjectInspectors = new ArrayList<>();
    outputObjectInspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                                                                   outputObjectInspectors);
  }

  private String readString(Object[] args, int i) {
    return ((StringObjectInspector) objectInspectors[i]).getPrimitiveJavaObject(args[i]).trim();
  }

  private List<String> readList(Object[] args, int i) {
    String str = readString(args, i);
    List<String> list = new ArrayList<>();
    if (!str.isEmpty()) {
      list.addAll(Arrays.asList(str.split(",")));
    }
    return list;
  }

  @Override
  public void configure(MapredContext mapredContext) {
    this.mapredContext = mapredContext;
  }

  @Override
  public void process(Object[] args) throws HiveException {
    // args:          0       1         2         3                 4         5       6         7     other
    // ak             true    ini path  endpoint  tunnel endpoint   project   table   columns   pts   col1... pt1...
    // bearer token   false   token     endpoint  tunnel endpoint   project   table
    try {
      if (odps == null) {
        // setup odps
        Account account;
        if ("AK".equals(readString(args, IDX_AUTH_TYPE))) {
          String odpsConfigPath = readString(args, IDX_ODPS_CONFIG_PATH);
          OdpsConfig odpsConfig = new OdpsConfig(mapredContext, odpsConfigPath);
          account = new AliyunAccount(odpsConfig.getAccessId(), odpsConfig.getAccessKey());
        } else if ("BearerToken".equals(readString(args, IDX_AUTH_TYPE))) {
          String bearerToken = readString(args, IDX_TOKEN);
          print("bearer token: " + bearerToken);
          account = new BearerTokenAccount(bearerToken);
        } else {
          throw new RuntimeException("Unsupported authorization type");
        }

        String endpoint = readString(args, IDX_ENDPOINT);
        print("endpoint: " + endpoint);
        odps = new Odps(account);
        odps.setEndpoint(endpoint);
        odps.setUserAgent("MMA");
        tunnel = new TableTunnel(odps);
        String tunnelEndpoint = readString(args, IDX_TUNNEL_ENDPOINT);
        print("tunnel endpoint: " + tunnelEndpoint);
        if (!tunnelEndpoint.isEmpty()) {
          tunnel.setEndpoint(tunnelEndpoint);
        }
      }

      if (odpsTableName == null) {
        // setup project, table, schema, value array
        odpsProjectName = readString(args, IDX_PROJECT);
        odps.setDefaultProject(odpsProjectName);
        print("project: " + odpsProjectName);

        odpsTableName = readString(args, IDX_TABLE);
        print("table: " + odpsTableName);
        schema = odps.tables().get(odpsTableName).getSchema();

        odpsColumnNames = readList(args, IDX_COLUMNS);
        hiveColumnValues = new Object[odpsColumnNames.size()];

        odpsPartitionColumnNames = readList(args, IDX_PTS);
        hivePartitionColumnValues = new Object[odpsPartitionColumnNames.size()];
      }

      // Step 1: get Hive column value and pt value
      for (int i = 0; i < odpsColumnNames.size(); i++) {
        hiveColumnValues[i] = args[i + IDX_COL_START];
      }

      IDX_PT_BEGIN = IDX_COL_START + odpsColumnNames.size();
      for (int i = 0; i < odpsPartitionColumnNames.size(); i++) {
        hivePartitionColumnValues[i] = args[i + IDX_PT_BEGIN];
      }

      // Step 2: get pt spec
      // Get partition spec
      String partitionSpec = getPartitionSpec();
      if (partitionSpec.contains("__HIVE_DEFAULT_PARTITION__")) {
        return;
      }

      // Create new tunnel upload session & record writer or reuse the current ones
      if (currentOdpsPartitionSpec == null || !currentOdpsPartitionSpec.equals(partitionSpec)) {
        resetUploadSession(partitionSpec);
      }

      // Step 3: set record and write to tunnel
      if (reusedRecord == null) {
        reusedRecord = currentUploadSession.newRecord();
      }

      for (int i = 0; i < odpsColumnNames.size(); i++) {
        String odpsColumnName = odpsColumnNames.get(i);
        Object value = hiveColumnValues[i];
        if (value == null) {
          reusedRecord.set(odpsColumnName, null);
        } else {
          // Handle data types
          ObjectInspector objectInspector = objectInspectors[i + IDX_COL_START];
          TypeInfo typeInfo = schema.getColumn(odpsColumnName).getTypeInfo();
          reusedRecord.set(odpsColumnName,
                           HiveObjectConverter.convert(objectInspector, value, typeInfo));
        }
      }

      recordWriter.write(reusedRecord);
      numRecordTransferred += 1;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  private String getPartitionSpec() {
    StringBuilder partitionSpecBuilder = new StringBuilder();
    for (int i = 0; i < odpsPartitionColumnNames.size(); ++i) {
      Object colValue = hivePartitionColumnValues[i];
      if (colValue == null) {
        continue;
      }

      ObjectInspector objectInspector = objectInspectors[i + IDX_PT_BEGIN];
      TypeInfo typeInfo = schema.getPartitionColumn(odpsPartitionColumnNames.get(i)).getTypeInfo();
      Object odpsValue = HiveObjectConverter.convert(objectInspector, colValue, typeInfo);

      partitionSpecBuilder.append(odpsPartitionColumnNames.get(i));
      partitionSpecBuilder.append("='");
      partitionSpecBuilder.append(odpsValue.toString()).append("'");
      if (i != odpsPartitionColumnNames.size() - 1) {
        partitionSpecBuilder.append(",");
      }
    }
    return partitionSpecBuilder.toString();
  }

  private void resetUploadSession(String partitionSpec)
      throws TunnelException, IOException, HiveException {
    // Close current record writer
    if (currentUploadSession != null) {
      long bytes = ((TunnelBufferedWriter) recordWriter).getTotalBytes();
      recordWriter.close();
      bytesTransferred += bytes;
    }

    currentUploadSession = getOrCreateUploadSession(partitionSpec);
    recordWriter = currentUploadSession.openBufferedWriter(true);
    ((TunnelBufferedWriter) recordWriter).setBufferSize(64 * 1024 * 1024);
    currentOdpsPartitionSpec = partitionSpec;
  }

  private UploadSession getOrCreateUploadSession(String partitionSpec)
      throws HiveException {
    UploadSession uploadSession = partitionSpecToUploadSession.get(partitionSpec);

    if (uploadSession == null) {
      int retry = 0;
      long sleep = 2000;
      while (true) {
        try {
          if (partitionSpec.isEmpty()) {
            print("creating record worker");
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
                                                       odpsTableName);
            print("creating record worker done");
          } else {
            print("creating record worker for " + partitionSpec);
            uploadSession = tunnel.createUploadSession(odps.getDefaultProject(),
                                                       odpsTableName,
                                                       new PartitionSpec(partitionSpec));
            print("creating record worker for " + partitionSpec + " done");
          }
          break;
        } catch (TunnelException e) {
          print("create session failed, retry: " + retry);
          e.printStackTrace(System.out);
          retry++;
          if (retry > 5) {
            throw new HiveException(e);
          }
          try {
            Thread.sleep(sleep + ThreadLocalRandom.current().nextLong(3000));
          } catch (InterruptedException ex) {
            ex.printStackTrace();
          }
          sleep = sleep * 2;
        }
      }
      partitionSpecToUploadSession.put(partitionSpec, uploadSession);
    }

    return uploadSession;
  }

  @Override
  public void close() throws HiveException {
    if (recordWriter == null) {
      print("record writer is null, seems no record is fed to this UDTF");
    } else {
      // TODO: rely on tunnel retry strategy once the RuntimeException bug is fixed
      int retry = 5;
      while (true) {
        try {
          long bytes = ((TunnelBufferedWriter) recordWriter).getTotalBytes();
          recordWriter.close();
          bytesTransferred += bytes;
          break;
        } catch (Exception e) {
          print("Failed to close record writer, retry: " + retry);
          e.printStackTrace(System.out);
          retry--;
          if (retry <= 0) {
            throw new HiveException(e);
          }
        }
      }
    }

    for (String partitionSpec : partitionSpecToUploadSession.keySet()) {
      // If the number of parallel commit is huge, commit could fail. So we retry 5 times for each
      // session
      int retry = 5;
      while (true) {
        try {
          print("committing " + partitionSpec);
          partitionSpecToUploadSession.get(partitionSpec).commit();
          print("committing " + partitionSpec + " done");
          break;
        } catch (IOException | TunnelException e) {
          print("committing" + partitionSpec + " failed, retry: " + retry);
          e.printStackTrace(System.out);
          retry--;
          if (retry <= 0) {
            throw new HiveException(e);
          }
        }
      }
    }

    print("total bytes: " + bytesTransferred);
    print("upload speed (in KB): " + bytesTransferred / (System.currentTimeMillis() - startTime));

    forwardObj[0] = numRecordTransferred;
    forward(forwardObj);
  }

  private static void print(String log) {
    System.out.println(String.format("[MMA %d] %s", System.currentTimeMillis(), log));
  }
}
