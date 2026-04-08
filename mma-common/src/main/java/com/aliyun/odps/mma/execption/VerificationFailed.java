package com.aliyun.odps.mma.execption;

public class VerificationFailed extends MMATaskInterruptException {
    public VerificationFailed() {}
    public VerificationFailed(String msg) {
        super(msg);
    }
}
