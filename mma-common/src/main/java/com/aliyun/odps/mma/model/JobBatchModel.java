package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.constant.JobBatchStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class JobBatchModel {
    private Integer id;
    private Integer jobId;
    private Integer batchId;
    private JobBatchStatus status;
    private String errMsg;
    private Date createTime;
}
