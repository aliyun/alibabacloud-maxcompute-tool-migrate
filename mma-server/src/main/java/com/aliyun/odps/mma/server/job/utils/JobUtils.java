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

package com.aliyun.odps.mma.server.job.utils;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.PartitionOrderType;
import com.aliyun.odps.mma.job.JobStatus;
import com.aliyun.odps.mma.server.meta.MetaManager;
import com.aliyun.odps.mma.server.meta.generated.Job;


public class JobUtils
{
  public static String generateJobId(boolean isSubJob) {
    StringBuilder jobIdBuilder = new StringBuilder();
    if (isSubJob) {
      jobIdBuilder.append("S_");
    }
    jobIdBuilder.append(UUID.randomUUID().toString().replace("-", ""));
    return jobIdBuilder.toString();
  }

  public static JobStatus getJobStatus(Job record, MetaManager metaManager) {
    JobStatus jobStatus = JobStatus.valueOf(record.getJobStatus());
    if (record.hasSubJob() && !metaManager.listSubJobs(record.getJobId()).isEmpty()) {
      List<Job> subJobs = metaManager.listSubJobs(record.getJobId());
      Map<JobStatus, Integer> subJobStatusToJobCount = new HashMap<>();
      for (Job subJob : subJobs) {
        JobStatus subJobStatus = JobStatus.valueOf(subJob.getJobStatus());
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

      if (canceled == subJobs.size()) {
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
      if (succeeded == subJobs.size()) {
        return JobStatus.SUCCEEDED;
      } else if (pending == subJobs.size()) {
        return JobStatus.PENDING;
      } else if (running > 0) {
        return JobStatus.RUNNING;
      } else if (pending > 0) {
        return JobStatus.PENDING;
      } else if (succeeded + failed + canceled == subJobs.size() && failed > 0) {
        return JobStatus.FAILED;
      } else {
        throw new IllegalStateException("Illegal sub job status: " + subJobStatusToJobCount);
      }
    } else {
      return jobStatus;
    }
  }

  public static boolean partitionFilter(JobConfiguration config, List<String> partitionValues) {
    List<String> partitionBegin = config.getPartitionBegin();
    List<String> partitionEnd = config.getPartitionEnd();
    List<PartitionOrderType> partitionOrders = config.getPartitionOrderType();

    // 1/2 == 1/2/3
    // begin      end       p
    // 1/2        2/1       1/2       p==begin && p<end
    // 1/2        1/2       1/2/3     p==begin && p==end
    // 1/2        1/2/3     1/2/3     p==begin && p==end
    // 1/2/3      1/2       1/2/3     p==begin && p==end
    Comparator<List<String>> cmp = (o1, o2) -> {
      int len1 = o1.size();
      int len2 = o2.size();
      int minLen = Math.min(len1, len2);

      int ret = 0;
      for (int i = 0; i < minLen; i++) {
        switch (partitionOrders.get(i)) {
          case num:
            ret = (int) (Double.parseDouble(o1.get(i)) - Double.parseDouble(o2.get(i)));
            break;
          case lex:
          default:
            ret = o1.get(i).compareTo(o2.get(i));
        }
        if (ret != 0) {
          return ret;
        }
      }
      return ret;
    };

    if(cmp.compare(partitionBegin, partitionEnd) > 0){
      List<String> tmp = partitionBegin;
      partitionBegin = partitionEnd;
      partitionEnd = tmp;
    }

    return cmp.compare(partitionValues, partitionBegin) >= 0
           && cmp.compare(partitionValues, partitionEnd) <= 0;
  }
}
