/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.aliyun.odps.datacarrier.taskscheduler.ui;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class PagedDataSource <T> {
  private static final Logger LOG = LogManager.getLogger(PagedDataSource.class);

  private int pageSize;

  public PagedDataSource(int pageSize) {
    if (pageSize <= 0) {
      throw new IllegalArgumentException("Page size must be positive");
    }
    this.pageSize = pageSize;
  }

  public abstract int dataSize();

  /**
   * Return the data of a given range.
   *
   * @param from Starts from 0. Included.
   * @param to Less than or equals to {@link #dataSize()}. Excluded.
   * @return
   */
  public abstract List<T> sliceData(int from, int to);

  public int totalPages() {
    return (dataSize() + pageSize - 1) / pageSize;
  }

  /**
   * Return the page data at given page number.
   * @param page Page number.
   * @return
   */
  public PageData<T> pageData(int page) {
    int totalPages = (dataSize() + pageSize - 1) / pageSize;

    // Empty page
    if (dataSize() == 0) {
      return new PageData<>(1, new LinkedList<>());
    }

    int from = Math.min(dataSize(), (page - 1) * pageSize);
    int to = Math.min(dataSize(), page * pageSize);
    return new PageData<>(totalPages, sliceData(from, to));
  }

  public int getPageSize() {
    return pageSize;
  }
}
