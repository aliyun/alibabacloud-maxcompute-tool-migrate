package com.aliyun.odps.mma.config.exception;

public class ConfigMissingException extends ConfigException {
    private final String configName;

    public ConfigMissingException(String configName) {
        super();
        this.configName = configName;
    }

    public String getConfigName() {
        return configName;
    }
}
