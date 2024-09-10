package com.aliyun.odps.mma.execption;

public class MMATaskInterruptException extends Exception {
    public MMATaskInterruptException() {
        super();
    }
    public MMATaskInterruptException(String msg) {
        super(msg);
    }
}
