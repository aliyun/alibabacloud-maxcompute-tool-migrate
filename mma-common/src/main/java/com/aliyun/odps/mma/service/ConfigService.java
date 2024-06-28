package com.aliyun.odps.mma.service;

import com.aliyun.odps.mma.model.ConfigItem;

import java.util.List;
import java.util.Objects;

public interface ConfigService {
    List<ConfigItem> getAllConfig();
    List<ConfigItem> getCategoryConfig(String category);
    String getConfig(String category, String name);
    void setConfig(String category, String name, String value);
    void insertConfig(String category, String name, String value);
    void deleteConfig(String category, String name);

    default String getConfigOrDefault(String category, String name, String defaultValue) {
        String value = this.getConfig(category, name);

        if (Objects.nonNull(value)) {
            return value;
        }

        return defaultValue;
    }

    default int getIntConfig(String category, String name) {
        String value = this.getConfig(category, name);
        return Integer.parseInt(value);
    }

    default long getLongConfig(String category, String name) {
        String value = this.getConfig(category, name);
        return Long.parseLong(value);
    }

    default void setIntConfig(String category, String name, int value) {
        this.setConfig(category, name, Integer.toString(value));
    }

    default void setLongConfig(String category, String name, long value) {
        this.setConfig(category, name, Long.toString(value));
    }

    List<ConfigItem> getTimers();
}
