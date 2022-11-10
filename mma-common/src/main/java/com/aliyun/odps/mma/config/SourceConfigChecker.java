package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.meta.MetaLoader;
import com.aliyun.odps.mma.meta.MetaLoaderUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

public abstract class SourceConfigChecker {
    protected MetaLoaderUtils metaLoaderUtils;

    @Autowired
    public void setMetaLoaderUtils(MetaLoaderUtils metaLoaderUtils) {
        this.metaLoaderUtils = metaLoaderUtils;
    }

    public Map<String, String> checkConfig(SourceConfig sourceConfig) throws Exception {
        MetaLoader metaLoader = this.metaLoaderUtils.getMetaLoader(sourceType());
        metaLoader.checkConfig(sourceConfig);

        return null;
    }

    abstract SourceType sourceType();
}
