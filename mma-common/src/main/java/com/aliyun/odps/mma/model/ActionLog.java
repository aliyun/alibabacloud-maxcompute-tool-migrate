package com.aliyun.odps.mma.model;

import com.aliyun.odps.mma.constant.ActionStatus;
import com.aliyun.odps.mma.constant.ActionType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ActionLog {
    private Integer id;
    private Integer sourceId;
    private ActionType actionType;
    private ActionStatus status;
    private String action;
    private String msg;
    private Date createTime;
    private Date updateTime;
}
