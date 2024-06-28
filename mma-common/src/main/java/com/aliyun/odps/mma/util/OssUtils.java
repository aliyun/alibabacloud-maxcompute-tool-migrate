package com.aliyun.odps.mma.util;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.*;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class OssUtils {
    private final OSS oss;

    public OssUtils(String endpoint, String accessId, String accessKey) {
        ClientBuilderConfiguration config = new ClientBuilderConfiguration();
//        config.setConnectionTimeout(5);
//        config.setConnectionRequestTimeout(5);
        this.oss = new OSSClientBuilder().build(endpoint, accessId, accessKey, config);
    }

    public boolean doesBucketExist(String bucket) {
        return oss.doesBucketExist(bucket);
    }

    public void  deleteObject(String bucket, String path) {
        oss.deleteObject(bucket, path);
    }

    public void  deleteDir(String bucket, String path) {
        deleteDir(bucket, path, null);
    }

    public void  deleteDir(String bucket, String path, List<String> whitelist) {
        // 删除目录及目录下的所有文件。
        String nextMarker = null;
        ObjectListing objectListing = null;

        do {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucket)
                    .withPrefix(path)
                    .withMarker(nextMarker);

            objectListing = oss.listObjects(listObjectsRequest);
            List<OSSObjectSummary> objectSummaries = objectListing.getObjectSummaries();

            if (! objectSummaries.isEmpty()) {
                List<String> keys1 = objectSummaries.stream().map(OSSObjectSummary::getKey).collect(Collectors.toList());
                Stream<String> keysStream = objectSummaries.stream().map(OSSObjectSummary::getKey);

                if (ListUtils.size(whitelist) > 0) {
                    keysStream = keysStream.filter(key-> {
                        for (String white : whitelist) {
                            if (key.startsWith(white)) {
                                return true;
                            }
                        }

                        return false;
                    });
                }

                List<String> keys = keysStream.collect(Collectors.toList());

                if (! keys.isEmpty()) {
                    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(bucket)
                            .withKeys(keys)
                            .withEncodingType("url");

                    oss.deleteObjects(deleteObjectsRequest);
                }
            }

            nextMarker = objectListing.getNextMarker();
        } while (objectListing.isTruncated());
    }
}
