package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.constant.TaskStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TaskLog {
    private Integer id;
    private Integer taskId;
    private TaskStatus status;
    private String action; // 如，执行的sql语句等
    private String msg;    // 如，select count(*)的返回值
    private Date createTime;
    private Date updateTime;
}
