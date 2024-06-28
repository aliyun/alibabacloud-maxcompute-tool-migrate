package com.aliyun.odps.mma.service.impl;

import com.aliyun.odps.mma.model.ConfigItem;
import com.aliyun.odps.mma.mapper.ConfigMapper;
import com.aliyun.odps.mma.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class ConfigServiceImpl implements ConfigService {
    private final ConfigMapper mapper;

    @Autowired
    public ConfigServiceImpl(ConfigMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public List<ConfigItem> getAllConfig() {
        return this.mapper.getAllConfig();
    }

    @Override
    public List<ConfigItem> getCategoryConfig(String category) {
        return this.mapper.getCategoryConfig(category);
    }

    @Override
    public String getConfig(String category, String name) {
        return this.mapper.getConfig(category, name);
    }

    @Override
    public void setConfig(String category, String name, String value) {
        if (Objects.isNull(value)) {
            return;
        }

        String oldValue = this.getConfig(category, name);
        if (Objects.isNull(oldValue)) {
            this.insertConfig(category, name, value);
            return;
        }

        if (value.equals(oldValue)) {
            return;
        }

        this.mapper.updateConfig(category, name, value);
    }

    @Override
    public void insertConfig(String category, String name, String value) {
        this.mapper.insertConfig(category, name, value);
    }

    @Override
    public void deleteConfig(String category, String name) {
        this.mapper.deleteConfig(category, name);
    }

    @Override
    public List<ConfigItem> getTimers() {
        return this.mapper.getTimers();
    }
}
