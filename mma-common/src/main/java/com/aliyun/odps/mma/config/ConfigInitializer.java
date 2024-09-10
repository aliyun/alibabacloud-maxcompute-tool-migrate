package com.aliyun.odps.mma.config;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;

import com.aliyun.odps.mma.service.ConfigService;

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

    @Bean(name="HIVE")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HiveConfig getHiveConfig() {
        HiveConfig hiveConfig = new HiveConfig();
        initConfig(hiveConfig);
        return hiveConfig;
    }

    @Bean(name="HIVE_OSS")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HiveOssConfig getOssConfig() {
        HiveOssConfig ossConfig = new HiveOssConfig();
        initConfig(ossConfig);
        return ossConfig;
    }

    @Bean(name="ODPS")
    @Primary
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public OdpsConfig getOdpsConfig() {
        OdpsConfig odpsConfig = new OdpsConfig();
        initConfig(odpsConfig);
        return odpsConfig;
    }

    @Bean(name="BIGQUERY")
    @Primary
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public BigQueryConfig getBigQueryConfig() {
        BigQueryConfig bigQueryConfig = new BigQueryConfig();
        initConfig(bigQueryConfig);
        return bigQueryConfig;
    }

    @Bean(name="DATABRICKS")
    @Primary
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public DatabricksConfig getDatabricksConfig() {
        DatabricksConfig config = new DatabricksConfig();
        initConfig(config);
        return config;
    }


    @Bean(name="HIVE_GLUE")
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public HiveGlueConfig getHiveGlueConfig() {
        HiveGlueConfig config = new HiveGlueConfig();
        initConfig(config);
        return config;
    }

//    @Bean(name="ODPS_OSS")
//    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
//    public OdpsOssConfig getOdpsOssConfig() {
//        OdpsOssConfig config = new OdpsOssConfig();
//        initConfig(config);
//
//        return config;
//    }

    private void initConfig(Config config) {
        config.setService(configService);
        Map<String, String> defaultValues = config.defaultValues;

        try {
            Class<?> c = config.getClass();

            // 先parent class的filed, 在本class的field，让本class filed默认值覆盖parent class field的默认值
            Field[][] fieldsArray = new Field[][] {
                    c.getSuperclass().getFields(),
                    c.getDeclaredFields()
            };

            for (Field[] fields: fieldsArray) {
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
            }
        } catch (Exception e) {
            //! unreachable
        }
    }
}
