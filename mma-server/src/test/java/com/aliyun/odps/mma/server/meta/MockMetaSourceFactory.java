/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
