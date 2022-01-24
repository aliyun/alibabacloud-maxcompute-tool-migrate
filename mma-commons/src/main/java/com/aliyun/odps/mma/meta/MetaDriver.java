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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class MetaDriver implements Driver {
  private Driver driver;
  public MetaDriver(Driver d) { this.driver = d; }

  @Override
  public boolean acceptsURL(String u) throws SQLException {
    return this.driver.acceptsURL(u);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return this.driver.getPropertyInfo(url, info);
  }

  @Override
  public int getMajorVersion() {
    return this.driver.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return this.driver.getMinorVersion();
  }

  @Override
  public boolean jdbcCompliant() {
    return this.driver.jdbcCompliant();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.driver.getParentLogger();
  }

  @Override
  public Connection connect(String u, Properties p) throws SQLException {
    return this.driver.connect(u, p);
  }
}
