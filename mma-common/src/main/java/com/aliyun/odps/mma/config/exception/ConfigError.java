package com.aliyun.odps.mma.config.exception;

public class ConfigError extends ConfigException {
    private final Exception inner;

    public ConfigError(Exception e) {
        super();
        this.inner = e;
    }

    public ConfigError(String msg, Exception e) {
        super(msg);
        this.inner = e;
    }

    public Exception getInnerException() {
        return inner;
    }
}
