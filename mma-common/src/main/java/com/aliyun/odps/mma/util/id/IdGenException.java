package com.aliyun.odps.mma.util.id;

public class IdGenException extends Exception {
    Throwable e;

    public IdGenException(Throwable e) {
        super(e);
    }
}
