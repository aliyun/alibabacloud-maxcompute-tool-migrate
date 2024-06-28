package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.config.exception.ConfigMissingException;
import com.aliyun.odps.mma.service.ConfigService;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.springframework.beans.factory.annotation.Autowired;

import java.lang.reflect.Field;
import java.util.*;

public abstract class Config {
    protected ConfigService service;
    protected Map<String, String> defaultValues = new HashMap<>();
    protected Map<String, ConfigItem> configItemMap;
    protected List<String> configKeys;
    protected List<ConfigItem> configItems;
    protected Map<String, String> mem;

    protected List<String> itemMasks() {
        return Collections.emptyList();
    }

    public Config() {
        configItemMap = new HashMap<>();
        configItems = new ArrayList<>();
        configKeys = new ArrayList<>();
        initConfigItemMap(this.getClass());
    }

    protected void initConfigItemMap(Class<? extends Config> thisC) {
        Field[] fields = thisC.getFields();
        List<String> masks = this.itemMasks();

        for (Field field: fields) {
            if (!field.isAnnotationPresent(ConfigItem.class)) {
                continue;
            }

            ConfigItem configItem = field.getAnnotation(ConfigItem.class);

            try {
                String configKey = (String) field.get(this);

                if (masks.contains(configKey)) {
                    continue;
                }

                configItems.add(configItem);
                configKeys.add(configKey);
                configItemMap.put(configKey, configItem);
            } catch (Exception e) {
                // !unreachable
            }
        }
    }

    abstract public String category();

    @Autowired
    public void setService(ConfigService configService) {
        this.service = configService;
    }

    /**
     * 开启内存模式，这时，配置的保存和读取都会直接读写内存，不会读写数据库
     */
    public void openMemMode() {
        mem = new HashMap<>();
    }

    public void dumpMem() {
        for (String key: mem.keySet()) {
            String value = mem.get(key);
            this.service.setConfig(category(), key, value);
        }
    }

    public void closeMemMode() {
        this.mem = null;
    }

    protected String getDefaultValue(String name) {
        return defaultValues.get(name);
    }

    public String getOrDefault(String name, String defaultValue) {
        String value = getConfig(name);
        if (Objects.nonNull(value) && !value.trim().isEmpty()) {
            return value;
        }

        return defaultValue;
    }

    public String getConfig(String name) {
        String value = null;

        if (Objects.nonNull(mem)) {
            value = mem.get(name);
        }

        if (Objects.nonNull(value)) {
            return value;
        }

        value = this.service.getConfig(category(), name);
        if (! Objects.isNull(value)) {
            return value;
        }

        return this.getDefaultValue(name);
    }

    public void setConfig(String name, String value) {
        if (Objects.nonNull(mem)) {
            this.mem.put(name, value);
        } else {
            this.service.setConfig(category(), name, value);
        }
    }

    public void deleteConfig(String name) {
        this.service.deleteConfig(category(), name);
    }

    public String getOrErr(String name) throws ConfigMissingException {
        String value = getConfig(name);

        if (Objects.isNull(value)) {
            throw new ConfigMissingException(name);
        }

        return value;
    }


    public Integer getInteger(String name) {
        return Integer.parseInt(getConfig(name));
    }

    public Long getLong(String name) {
        return Long.parseLong(getConfig(name));
    }

    public Boolean getBoolean(String name) {
        return "true".equals(getConfig(name));
    }

    public List<String> getList(String name) {
        String value = getConfig(name);
        if (Objects.isNull(value)) {
            return new ArrayList<>();
        }
        Gson gson = new Gson();
        return gson.fromJson(value, new TypeToken<List<String>>() {}.getType());
    }

    public Map<String, String> getMap(String name) {
        String value = getConfig(name);
        if (Objects.isNull(value)) {
            return new HashMap<>();
        }
        Gson gson = new Gson();
        return gson.fromJson(value, new TypeToken<Map<String, String>>() {}.getType());
    }

    public TimerConfig getTimer(String name) {
        String value = getConfig(name);
        if (Objects.isNull(value)) {
            return new TimerConfig();
        }

        return TimerConfig.parse(value);
    }

    public void setList(String name, List<String> value) {
        Gson gson = new Gson();
        String json = gson.toJson(value);
        setConfig(name, json);
    }

    public void setInteger(String name, int value) {
        setConfig(name, Integer.toString(value));
    }

    public void setBoolean(String name, boolean value) {
        setConfig(name, value ? "true" : "false");
    }

    public List<Map<String, Object>> toJsonObj() {
        List<Map<String, Object>> items = new ArrayList<>();

        for (int i = 0, n = configItems.size(); i < n; i ++) {
            String configKey = configKeys.get(i);
            ConfigItem configItem = configItems.get(i);
            Map<String, Object> item = new HashMap<>();

            Object configValue = null;

            if (Objects.nonNull(this.getConfig(configKey))) {
                switch (configItem.type()) {
                    case "int":
                        configValue = this.getInteger(configKey);
                        break;
                    case "long":
                        configValue = this.getLong(configKey);
                        break;
                    case "list":
                        configValue = this.getList(configKey);
                        break;
                    case "map":
                        configValue = this.getMap(configKey);
                        break;
                    case "boolean":
                        configValue = this.getBoolean(configKey);
                        break;
                    case "timer":
                        configValue = this.getMap(configKey);
                        break;
                    default:
                        configValue = this.getConfig(configKey);
                        break;
                }
            }

            item.put("key", configKey);
            if (Objects.nonNull(configValue) && !Objects.equals(configItem.type(), "password")) {
                item.put("value", configValue);
            }

            if (configItem.required()) {
                item.put("required", true);
            }

            if (! configItem.editable()) {
                if (Objects.isNull(configValue)) {
                    continue;
                }

                item.put("editable", false);
            }

            item.put("type", configItem.type());
            item.put("desc", configItem.desc());
            items.add(item);
        }

        return items;
    }

    public String toJson() {
        return toJson(false);
    }

    public String toJson(boolean pretty) {
        List<Map<String, Object>> items = toJsonObj();

        GsonBuilder gb = new GsonBuilder();
        if (pretty) {
            gb.setPrettyPrinting();
        }

        Gson gson = gb.create();

        return gson.toJson(items);
    }

    public Map<String, String> addConfigItems(Map<String, Object> items)  {
        Gson gson = new Gson();
        Map<String, String> errors = new HashMap<>();
        Map<String, String> clearItems = new HashMap<>();
        List<String> itemsDeleted = new ArrayList<>();

        for (String configKey: items.keySet()) {
            ConfigItem configItem = configItemMap.get(configKey);
            if (Objects.isNull(configItem)) {
                continue;
            }

            Object configValue = items.get(configKey);
            if (Objects.isNull(configValue)) {
                itemsDeleted.add(configKey);
                continue;
            }

            String configStrVal;

            if (configValue instanceof String) {
                configStrVal = (String) configValue;
                configStrVal = configStrVal.trim();

                if (configStrVal.isEmpty()) {
                    itemsDeleted.add(configKey);
                    continue;
                }
            } else {
                configStrVal = gson.toJson(configValue);
            }

            configStrVal = configStrVal.trim();

            switch (configItem.type()) {
                case "int":
                    try {
                        Integer.parseInt(configStrVal);
                    } catch (Exception e) {
                        errors.put(configKey, "is not a number");
                    }

                    break;
                case "long":
                    try {
                        Long.parseLong(configStrVal);
                    } catch (Exception e) {
                        errors.put(configKey, "is not a long number");
                    }
                    break;
                case "list":
                    if (! (configValue instanceof List)) {
                        errors.put(configKey, "is not a valid json of string list");
                    } else {
                        List<?> listValue = (List<?>) configValue;
//
//                        if (listValue.size() == 0) {
//                            itemsDeleted.add(configKey);
//                            continue;
//                        }

                        for (Object i: listValue) {
                            if (! (i instanceof String)) {
                                errors.put(configKey, "is not a valid json of string list");
                                break;
                            }
                        }
                    }

                    break;
                case "map":
                    if (! (configValue instanceof Map)) {
                        errors.put(configKey, "is not a valid json of Map");
                    }
                    break;
                case "timer":
                    if (! (configValue instanceof Map)) {
                        errors.put(configKey, "is not a valid timer value");
                    } else {
                        String error = TimerConfig.verifyConfig((Map<String, String>)configValue);
                        if (StringUtils.isNotBlank(error)) {
                            errors.put(configKey, error);
                        }
                    }

                    break;
                case "boolean":
                    if ((!"true".equals(configStrVal)) && (!"false".equals(configStrVal))) {
                        errors.put(configKey, "should be true or false");
                    }
                    break;
                case "string":
                    String[] enums = configItem.enums();
                    if (enums.length > 0) {
                        List<String> enumList = Arrays.asList(enums);

                        if (! enumList.contains(configStrVal)) {
                            errors.put(configKey, "should be one of [" + String.join(", ", enumList) + "]");
                        }
                    }
                    break;
                default:
                    break;
            }

            clearItems.put(configKey, configStrVal);
        }

        if (errors.size() > 0) {
            return errors;
        }

        for (String configKey: clearItems.keySet()) {
            String configValue = clearItems.get(configKey);
            this.setConfig(configKey, configValue);
        }

        for (String configKey: itemsDeleted) {
            this.deleteConfig(configKey);
        }

        return errors;
    }

    public Map<String, String> initFromMap(Map<String, Object> items) {
        Map<String, String> errors = new HashMap<>();

        for (String configKey: configItemMap.keySet()) {
            ConfigItem configItem = configItemMap.get(configKey);

            if (configItem.required() && !items.containsKey(configKey)) {
                errors.put(configKey, "cannot be empty");
                continue;
            }

            // string为空的情况
            if (configItem.required() && items.containsKey(configKey)) {
                Object configValue = items.get(configKey);

                if (configValue instanceof String && ((String) configValue).trim().isEmpty()) {
                    errors.put(configKey, "cannot be empty");
                    items.remove(configKey);
                }
            }
        }

        errors.putAll(addConfigItems(items));

        return errors;
    }
}
