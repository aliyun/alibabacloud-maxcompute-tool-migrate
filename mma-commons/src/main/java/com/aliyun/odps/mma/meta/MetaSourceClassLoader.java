/*
 * Copyright 1999-2022 Alibaba Group Holding Ltd.
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
 *
 */

package com.aliyun.odps.mma.meta;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandler;

import org.apache.commons.lang3.StringUtils;

public class MetaSourceClassLoader extends URLClassLoader {

  /**
   *
   * @param connectorJarPath jar path, eg: lib/connector/hive-1.1.0.jar
   * @return
   * @throws IOException
   */
  public static MetaSourceClassLoader getInstance(String connectorJarPath) throws IOException {
    if (StringUtils.isBlank(connectorJarPath)) {
      throw new IOException("Connector JAR is not specified");
    }
    URL[] urls = new URL[1];
    String repo = (new URL("file", null, connectorJarPath)).toString();
    URLStreamHandler streamHandler = null;
    urls[0] = new URL(null, repo, streamHandler);

    return new MetaSourceClassLoader(urls, MetaSourceFactory.class.getClassLoader());
  }

  public MetaSourceClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    Thread.currentThread().setContextClassLoader(this);
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      Class<?> c = findLoadedClass(name);
      // Class<?> c = null;
      if (c == null) {
        try {
          c = findClass(name);
        } catch (ClassNotFoundException e) {
        }

        if (c == null) {
          c = super.loadClass(name);
        }
      }
      return c;
    }
  }

}