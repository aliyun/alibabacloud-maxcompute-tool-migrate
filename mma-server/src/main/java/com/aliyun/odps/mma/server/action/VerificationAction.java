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

package com.aliyun.odps.mma.server.action;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.server.action.info.VerificationActionInfo;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSource.TableMetaModel;
import com.aliyun.odps.mma.server.task.Task;

public class VerificationAction extends DefaultAction {
  private static final Logger LOG = LogManager.getLogger(VerificationAction.class);

  private TableMetaModel tableMetaModel;

  public VerificationAction(
      String id,
      TableMetaModel tableMetaModel,
      Task task,
      ActionExecutionContext context) {
    super(id, task, context);
    this.tableMetaModel = tableMetaModel;
    actionInfo = new VerificationActionInfo();
  }

  @Override
  public Object call() {
    List<List<Object>> sourceVerificationResult =
        actionExecutionContext.getSourceVerificationResult();
    List<List<Object>> destVerificationResult =
        actionExecutionContext.getDestVerificationResult();
    VerificationActionInfo actionInfo = (VerificationActionInfo) getActionInfo();

    boolean passed = true;
    int partitionColumnCount = tableMetaModel.getPartitionColumns().size();
    boolean isPartitioned = !tableMetaModel.getPartitionColumns().isEmpty();
    actionInfo.setIsPartitioned(isPartitioned);

    if (sourceVerificationResult == null || destVerificationResult == null) {
      LOG.error("ActionId: {}, source/dest verification results not found", id);
      passed = false;
    } else if (!isPartitioned) {
      assert sourceVerificationResult.size() == 1;
      assert sourceVerificationResult.get(0).size() == 1;
      assert destVerificationResult.size() == 1;
      assert sourceVerificationResult.get(0).size() == 1;

      Long source = (long) sourceVerificationResult.get(0).get(0);
      Long dest = (long) destVerificationResult.get(0).get(0);
      actionInfo.setSourceNumRecord(source);
      actionInfo.setDestNumRecord(dest);
      passed = source.equals(dest);
      if (!passed) {
        LOG.error("ActionId: {}, verification failed, source: {}, dest: {}",
                  id, source, dest);
      } else {
        LOG.info("ActionId: {}, verification succeeded, source: {}, dest: {}",
                 id, source, dest);
      }
    } else {
      List<String> succeededPartitions = new LinkedList<>();
      List<String> failedPartitions = new LinkedList<>();

      for (MetaSource.PartitionMetaModel partitionMetaModel : tableMetaModel.getPartitions()) {
        List<String> partitionValues = partitionMetaModel.getPartitionValues();
        String partitionValuesStr = partitionValues.toString();
        List<Object> sourceRecordCount = sourceVerificationResult
            .stream()
            .filter(r -> partitionValues.equals(r.subList(0, partitionColumnCount)))
            .map(r -> r.get(partitionColumnCount))
            .collect(Collectors.toList());
        List<Object> destRecordCount = destVerificationResult
            .stream()
            .filter(r -> partitionValues.equals(r.subList(0, partitionColumnCount)))
            .map(r -> r.get(partitionColumnCount))
            .collect(Collectors.toList());

        // When partition is empty, foundInSource and foundInDest are both false.
        if (sourceRecordCount.isEmpty() && destRecordCount.isEmpty()) {
          LOG.warn("ActionId: {}, ignored Empty partition: {}, ", id, partitionValues);
          actionInfo.setPartitionValuesToSourceNumRecord(partitionValuesStr, null);
          actionInfo.setPartitionValuesToDestNumRecord(partitionValuesStr, null);
          succeededPartitions.add(partitionValuesStr);
          continue;
        } if (sourceRecordCount.isEmpty()) {
          actionInfo.setPartitionValuesToSourceNumRecord(partitionValuesStr, null);
          actionInfo.setPartitionValuesToDestNumRecord(
              partitionValuesStr,
              (long) destRecordCount.get(0));
          LOG.warn("ActionId: {}, ignored unexpected partition: {}", id, partitionValues);
          succeededPartitions.add(partitionValuesStr);
          continue;
        }  if (destRecordCount.isEmpty()) {
          LOG.error("ActionId: {}, dest partition not found, partition: {}",
                    id, partitionValues);
          actionInfo.setPartitionValuesToSourceNumRecord(
              partitionValuesStr,
              (long) sourceRecordCount.get(0));
          actionInfo.setPartitionValuesToDestNumRecord(partitionValuesStr, null);
          failedPartitions.add(partitionValuesStr);
          passed = false;
          continue;
        }
        Long source = (long) sourceRecordCount.get(0);
        Long dest = (long) destRecordCount.get(0);
        actionInfo.setPartitionValuesToSourceNumRecord(partitionValuesStr, source);
        actionInfo.setPartitionValuesToDestNumRecord(partitionValuesStr, dest);
        if (!dest.equals(source)) {
          LOG.error("ActionId: {}, verification failed, source: {}, dest: {}",
                    id, source, dest);
          passed = false;
          failedPartitions.add(partitionValuesStr);
          continue;
        }
        LOG.info("ActionId: {}, verification succeeded, source: {}, dest: {}",
                 id, source, dest);
        succeededPartitions.add(partitionValuesStr);
      }

      actionInfo.setSucceededPartitions(succeededPartitions);
      actionInfo.setFailedPartitions(failedPartitions);
    }

    if (passed) {
      actionInfo.setPassed(Boolean.TRUE);
    } else {
      actionInfo.setPassed(Boolean.FALSE);
      throw new RuntimeException("Verification failed");
    }
    return null;
  }

  @Override
  public String getName() {
    return "Final verification";
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  void handleResult(Object result) {
  }
}
