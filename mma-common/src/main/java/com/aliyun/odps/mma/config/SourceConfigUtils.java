package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SourceConfigUtils {
    ApplicationContext appCtx;
    Map<SourceType, Class<? extends SourceConfig >> typeToConfigClass;

    @Autowired
    public SourceConfigUtils(ApplicationContext appCtx, List<SourceConfig> sourceConfigs) {
        this.appCtx = appCtx;

        typeToConfigClass = new HashMap<>(sourceConfigs.size());
        for (SourceConfig s: sourceConfigs) {
            typeToConfigClass.put(s.getSourceType(), s.getClass());
        }
    }

    public SourceConfig newSourceConfig(SourceType sourceType, String sourceName) {
        SourceConfig sourceConfig = appCtx.getBean(typeToConfigClass.get(sourceType));
        sourceConfig.setSourceName(sourceName);
        return sourceConfig;
    }
}
