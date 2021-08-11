package com.aliyun.odps.mma.server.meta;

import com.aliyun.odps.mma.config.AbstractConfiguration;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSourceFactory;

public class MockMetaSourceFactory extends MetaSourceFactory {

  public MockMetaSourceFactory() {
    super();
  }

  @Override
  public MetaSource getMetaSource(AbstractConfiguration config) {
    String metadataSourceType = config.get(JobConfiguration.METADATA_SOURCE_TYPE);
    switch (metadataSourceType) {
      case "MaxCompute":
      case "Hive":
        return new MockMetaSource();
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata source type: " + metadataSourceType);
    }
  }
}
