package com.aliyun.odps.mma.config;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class DefaultConfig {
    Map<String, String> defaultValues = new HashMap<>();

    public String get(String key) {
        return defaultValues.get(key);
    }

    public void set(String key, String value) {
        defaultValues.put(key, value);
    }
}
