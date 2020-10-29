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

package com.aliyun.odps.datacarrier.taskscheduler.action;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.action.info.VerificationActionInfo;
import com.aliyun.odps.datacarrier.taskscheduler.meta.MetaSource.PartitionMetaModel;
import com.aliyun.odps.datacarrier.taskscheduler.MmaException;

public class VerificationAction extends AbstractAction {

  private static final Logger LOG = LogManager.getLogger(VerificationAction.class);

  public VerificationAction(String id) {
    super(id);
    actionInfo = new VerificationActionInfo();
  }

  @Override
  public void execute() throws MmaException {
    setProgress(ActionProgress.RUNNING);

    List<List<String>> sourceVerificationResult =
        actionExecutionContext.getSourceVerificationResult();
    List<List<String>> destVerificationResult =
        actionExecutionContext.getDestVerificationResult();
    VerificationActionInfo actionInfo = (VerificationActionInfo) getActionInfo();

    boolean passed = true;
    int partitionColumnCount = actionExecutionContext.getTableMetaModel().partitionColumns.size();
    boolean isPartitioned = partitionColumnCount != 0;
    actionInfo.setIsPartitioned(isPartitioned);


    if (sourceVerificationResult == null || destVerificationResult == null) {
      LOG.error("ActionId: {}, source/dest verification results not found", id);
      passed = false;
    } else {
      if (!isPartitioned) {
        assert sourceVerificationResult.size() == 1;
        assert sourceVerificationResult.get(0).size() == 1;
        assert destVerificationResult.size() == 1;
        assert sourceVerificationResult.get(0).size() == 1;

        Long source = Long.valueOf(sourceVerificationResult.get(0).get(0));
        Long dest = Long.valueOf(destVerificationResult.get(0).get(0));
        passed = source.equals(dest);
        if (!passed) {
          LOG.error("ActionId: {}, verification failed, source: {}, dest: {}",
                    id, source, dest);
        } else {
          LOG.info("ActionId: {}, verification succeeded, source: {}, dest: {}",
                   id, source, dest);
        }
      } else {
        List<List<String>> succeededPartitions = new LinkedList<>();
        List<List<String>> failedPartitions = new LinkedList<>();

        for (PartitionMetaModel partitionMetaModel :
            actionExecutionContext.getTableMetaModel().partitions) {

          List<String> partitionValues = partitionMetaModel.partitionValues;
          List<String> sourceRecordCount = sourceVerificationResult
              .stream()
              .filter(r -> partitionValues.equals(r.subList(0, partitionColumnCount)))
              .map(r -> r.get(partitionColumnCount))
              .collect(Collectors.toList());
          List<String> destRecordCount = destVerificationResult
              .stream()
              .filter(r -> partitionValues.equals(r.subList(0, partitionColumnCount)))
              .map(r -> r.get(partitionColumnCount))
              .collect(Collectors.toList());

          // When partition is empty, foundInSource and foundInDest are both false.
          if (sourceRecordCount.isEmpty() && destRecordCount.isEmpty()) {
            LOG.warn("ActionId: {}, ignored Empty partition: {}, ", id, partitionValues);
            succeededPartitions.add(partitionValues);
          } else if (sourceRecordCount.isEmpty()) {
            LOG.warn("ActionId: {}, ignored unexpected partition: {}", id, partitionValues);
            succeededPartitions.add(partitionValues);
          } else if (destRecordCount.isEmpty()) {
            LOG.error("ActionId: {}, dest partition not found, partition: {}",
                      id, partitionValues);
            failedPartitions.add(partitionValues);
            passed = false;
          } else {
            Long source = Long.valueOf(sourceRecordCount.get(0));
            Long dest = Long.valueOf(destRecordCount.get(0));
            if (!dest.equals(source)) {
              LOG.error("ActionId: {}, verification failed, source: {}, dest: {}",
                        id, source, dest);
              passed = false;
              failedPartitions.add(partitionValues);
            } else {
              LOG.info("ActionId: {}, verification succeeded, source: {}, dest: {}",
                        id, source, dest);
              succeededPartitions.add(partitionValues);
            }
          }
        }

        actionInfo.setSucceededPartitions(succeededPartitions);
        actionInfo.setFailedPartitions(failedPartitions);
      }
    }

    if (passed) {
      actionInfo.setPassed(true);
      setProgress(ActionProgress.SUCCEEDED);
    } else {
      actionInfo.setPassed(false);
      setProgress(ActionProgress.FAILED);
    }
  }

  @Override
  public String getName() {
    return "Final verification";
  }

  @Override
  public void afterExecution() {
  }

  @Override
  public boolean executionFinished() {
    return true;
  }

  @Override
  public void stop() {
  }
}
