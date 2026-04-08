package com.aliyun.odps.mma.utils;

public class RestException extends Exception {
    public RestException(String msg) {
        super(msg);
    }

    public RestException(Throwable cause) {
        super(cause);
    }

    public static RestException Error(String url, int httpCode, String errMsg) {
        String msg = String.format("failed to request %s with code=%d, msg=%s", url, httpCode, errMsg);

        return new RestException(msg);
    }
}
