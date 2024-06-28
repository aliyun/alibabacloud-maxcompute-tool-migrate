package com.aliyun.odps.mma.execption;

public class LoadMetaException extends Exception {
    public LoadMetaException(String msg) {
        super(msg);
    }

    public LoadMetaException(String msg, Throwable e) {
        super(msg, e);
    }
}
