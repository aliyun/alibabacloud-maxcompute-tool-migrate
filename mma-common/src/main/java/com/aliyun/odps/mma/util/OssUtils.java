package com.aliyun.odps.mma.util;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;


public class OssUtils {
    OSS oss;

    public OssUtils(String endpoint, String accessId, String accessKey) {
        ClientBuilderConfiguration config = new ClientBuilderConfiguration();
//        config.setConnectionTimeout(5);
//        config.setConnectionRequestTimeout(5);
        this.oss = new OSSClientBuilder().build(endpoint, accessId, accessKey, config);
    }

    public boolean doesBucketExist(String bucket) {
        return oss.doesBucketExist(bucket);
    }
}
