package com.aliyun.odps.mma.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.aliyun.odps.mma.constant.SourceType;

@Component
public class SourceConfigUtils {
    ApplicationContext appCtx;

    @Autowired
    public SourceConfigUtils(ApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    public SourceConfig newSourceConfig(SourceType sourceType, String sourceName) {
        SourceConfig sourceConfig = (SourceConfig) appCtx.getBean(sourceType.name());
        sourceConfig.setSourceName(sourceName);
        return sourceConfig;
    }
}
