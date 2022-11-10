package com.aliyun.odps.mma.meta;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import com.aliyun.odps.mma.config.HiveOssConfig;
import com.aliyun.odps.mma.util.OssUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.config.SourceConfig;
import com.aliyun.odps.mma.constant.SourceType;
import com.aliyun.odps.mma.meta.schema.MMATableSchema;
import com.aliyun.odps.mma.model.DataBaseModel;
import com.aliyun.odps.mma.model.ModelBase;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.model.TableModel;
import com.aliyun.odps.mma.util.DateUtils;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HiveOssMetaLoader extends HiveMetaLoader{
    @Override
    public void checkConfig(SourceConfig sourceConfig) throws Exception {
        HiveOssConfig config = (HiveOssConfig) sourceConfig;
        OssUtils ossUtils = new OssUtils(
                config.getConfig(HiveOssConfig.OSS_ENDPOINT_EXTERNAL),
                config.getConfig(HiveOssConfig.OSS_AUTH_ACCESS_ID),
                config.getConfig(HiveOssConfig.OSS_AUTH_ACCESS_KEY)
        );

        String bucket = config.getConfig(HiveOssConfig.OSS_BUCKET);
        if (!ossUtils.doesBucketExist(bucket)) {
            throw new Exception(String.format("bucket: %s does not exist", bucket));
        }

        super.checkConfig(sourceConfig);
    }

    @Override
    public SourceType sourceType() {
        return SourceType.HIVE_OSS;
    }
}
