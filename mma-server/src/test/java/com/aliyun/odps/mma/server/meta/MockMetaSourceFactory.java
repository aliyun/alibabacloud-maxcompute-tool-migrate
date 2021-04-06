package com.aliyun.odps.mma.server.meta;

import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.meta.MetaSource;
import com.aliyun.odps.mma.meta.MetaSourceFactory;

public class MockMetaSourceFactory extends MetaSourceFactory {

  public MockMetaSourceFactory() {
    super();
  }

  @Override
  public MetaSource getMetaSource(JobConfiguration config) {
    String metadataSourceType = config.get(JobConfiguration.METADATA_SOURCE_TYPE);
    switch (metadataSourceType) {
      case "MaxCompute": {
        return new MockMcMetaSource();
      }
      case "Hive":
      case "OSS":
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata source type: " + metadataSourceType);
    }
  }
}
