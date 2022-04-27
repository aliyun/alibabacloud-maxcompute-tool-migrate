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

package com.aliyun.odps.mma.server.job.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.mma.config.ConfigurationUtils;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.ObjectType;
import com.aliyun.odps.mma.config.PartitionOrderType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.meta.generated.JobRecord;


public class JobUtils
{
  public static HashMap<ObjectType, Set<String>> getObjectFilterList(String blacklistStr) {
    HashMap<ObjectType, Set<String>> blackList = new HashMap<>();
    if (StringUtils.isNotBlank(blacklistStr)) {
      // TABLE:t1,t2,t3...;RESOURCE:r1,r2,r3;...
      blacklistStr = blacklistStr.replaceAll("\\s+", "");
      List<String> blacklist = Arrays.asList(blacklistStr.split(";"));
      for (String item : blacklist) {
        String[] parts = item.split(":");
        ObjectType objectType = ObjectType.valueOf(parts[0]);
        Set<String> set = new HashSet<>(Arrays.asList(parts[1].split(",")));
        blackList.put(objectType, set);
      }
    }
    return blackList;
  }

  public static boolean intersectionIsNotNull(Map<ObjectType, Set<String>> blackList,
                                              Map<ObjectType, Set<String>> whiteList) {
    if (null == blackList || null == whiteList || blackList.isEmpty() || whiteList.isEmpty()) {
      return false;
    }
    for (Map.Entry<ObjectType, Set<String>> entry : blackList.entrySet()) {
      Set<String> intersection = new HashSet<>(entry.getValue());
      intersection.retainAll(whiteList.get(entry.getKey()));
      if (!intersection.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static String generateJobId(boolean isSubJob) {
    StringBuilder jobIdBuilder = new StringBuilder();
    if (isSubJob) {
      jobIdBuilder.append("S_");
    }
    jobIdBuilder.append(UUID.randomUUID().toString().replace("-", ""));
    return jobIdBuilder.toString();
  }

  public static JobStatus getJobStatus(JobRecord record, MetaManager metaManager) {
    JobStatus jobStatus = JobStatus.valueOf(record.getJobStatus());
    if (record.hasSubJob() && !metaManager.listSubJobs(record.getJobId()).isEmpty()) {
      List<JobRecord> subJobRecords = metaManager.listSubJobs(record.getJobId());
      Map<JobStatus, Integer> subJobStatusToJobCount = new HashMap<>();
      for (JobRecord subJobRecord : subJobRecords) {
        JobStatus subJobStatus = JobStatus.valueOf(subJobRecord.getJobStatus());
        if (!subJobStatusToJobCount.containsKey(subJobStatus)) {
          subJobStatusToJobCount.put(subJobStatus, 0);
        }
        subJobStatusToJobCount.put(subJobStatus, subJobStatusToJobCount.get(subJobStatus) + 1);
      }

      int succeeded = subJobStatusToJobCount.getOrDefault(JobStatus.SUCCEEDED, 0);
      int failed = subJobStatusToJobCount.getOrDefault(JobStatus.FAILED, 0);
      int pending = subJobStatusToJobCount.getOrDefault(JobStatus.PENDING, 0);
      int running = subJobStatusToJobCount.getOrDefault(JobStatus.RUNNING, 0);
      int canceled = subJobStatusToJobCount.getOrDefault(JobStatus.CANCELED, 0);

      if (canceled == subJobRecords.size()) {
        return JobStatus.CANCELED;
      } else if (canceled != 0) {
        throw new IllegalStateException("Illegal sub job status dist: " + subJobStatusToJobCount);
      }

      /**
       * S, F, C, P, R
       * SF, SC, SP, SR, FC, FP, FR, CP, CR, PR
       * SFC, SFP, SFR, FCP, FCR, CPR
       * SFCP, FCPR
       * SFCPR
       */

      // SUCCEEDED: S
      // FAILED: F, SF, FC, SFC
      // RUNNING:
      // PENDING: SR, FR, PR, SFR, FCR,
      // CANCELED: C, SC
      if (succeeded == subJobRecords.size()) {
        return JobStatus.SUCCEEDED;
      } else if (pending == subJobRecords.size()) {
        return JobStatus.PENDING;
      } else if (running > 0) {
        return JobStatus.RUNNING;
      } else if (pending > 0) {
        return JobStatus.PENDING;
      } else if (succeeded + failed + canceled == subJobRecords.size() && failed > 0) {
        return JobStatus.FAILED;
      } else {
        throw new IllegalStateException("Illegal sub job status: " + subJobStatusToJobCount);
      }
    } else {
      return jobStatus;
    }
  }

  public static class PartitionFilter {
    List<String> partitionBegin;
    List<String> partitionEnd;
    List<PartitionOrderType> partitionOrders;
    Comparator<List<String>> cmp;

    public PartitionFilter(JobConfiguration config) {
      this.partitionBegin = config.getPartitionBegin();
      this.partitionEnd = config.getPartitionEnd();
      this.partitionOrders = config.getPartitionOrderType();
      this.cmp = new ConfigurationUtils.PartitionComparator(partitionOrders);
    }

    public boolean filter(List<String> partitionValues) {
      return cmp.compare(partitionValues, partitionBegin) >= 0
             && cmp.compare(partitionValues, partitionEnd) <= 0;
    }
  }
}
