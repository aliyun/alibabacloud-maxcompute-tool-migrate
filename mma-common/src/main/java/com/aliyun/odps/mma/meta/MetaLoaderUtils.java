package com.aliyun.odps.mma.meta;

import com.aliyun.odps.mma.constant.SourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MetaLoaderUtils {
    ApplicationContext appCtx;
    Map<SourceType, Class<? extends MetaLoader>> typeMap = new HashMap<>();

    @Autowired
    public MetaLoaderUtils(ApplicationContext appCtx, List<MetaLoader> metaLoaderList) {
        this.appCtx = appCtx;

        for (MetaLoader metaLoader: metaLoaderList) {
            typeMap.put(metaLoader.sourceType(), metaLoader.getClass());
        }
    }

    public MetaLoader getMetaLoader(SourceType sourceType) {
        return appCtx.getBean(typeMap.get(sourceType));
    }
}
