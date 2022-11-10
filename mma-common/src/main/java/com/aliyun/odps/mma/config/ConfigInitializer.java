package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;

@Configuration
public class ConfigInitializer {
    ConfigService configService;
    ApplicationContext appCtx;
    DefaultConfig defaultConfig;

    @Autowired
    public ConfigInitializer(ApplicationContext appCtx) {
        this.appCtx = appCtx;
        this.configService = appCtx.getBean(ConfigService.class);
        this.defaultConfig = appCtx.getBean(DefaultConfig.class);
    }

    @Bean
    @Primary
    public MMAConfig getMMAConfig() {
        MMAConfig config = new MMAConfig();
        initConfig(config);
        return config;
    }

    @Bean
    @Primary
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HiveConfig getHiveConfig() {
        HiveConfig hiveConfig = new HiveConfig();
        initConfig(hiveConfig);
        return hiveConfig;
    }

    @Bean
//    @Primary
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HiveOssConfig getOssConfig() {
        HiveOssConfig ossConfig = new HiveOssConfig();
        initConfig(ossConfig);
        return ossConfig;
    }

    @Bean
    @Primary
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OdpsConfig getOdpsConfig() {
        OdpsConfig odpsConfig = new OdpsConfig();
        initConfig(odpsConfig);
        return odpsConfig;
    }

    private void initConfig(Config config) {
        config.setService(configService);
        Map<String, String> defaultValues = config.defaultValues;

        try {
            Class<?> c = config.getClass();
            Field[] fields = c.getFields();

            for (Field field: fields) {
                if (! field.isAnnotationPresent(ConfigItem.class)) {
                    continue;
                }

                field.setAccessible(true);

                ConfigItem configItem = field.getAnnotation(ConfigItem.class);
                String defaultValue = configItem.defaultValue();
                if (! Objects.equals("", defaultValue)) {
                    String configKey = (String) field.get(config);
                    defaultValues.put(configKey, configItem.defaultValue());
                }
            }
        } catch (Exception e) {
            //! unreachable
        }
    }
}
