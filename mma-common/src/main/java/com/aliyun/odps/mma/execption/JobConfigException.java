package com.aliyun.odps.mma.execption;

import lombok.Getter;

public class JobConfigException extends JobSubmittingException {
    @Getter
    private final String field;
    @Getter
    private final String errMsg;

    public JobConfigException(String field, String errMsg) {
        super(errMsg);
        this.field = field;
        this.errMsg = errMsg;
    }
}
