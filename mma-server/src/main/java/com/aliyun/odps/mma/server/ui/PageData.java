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

package com.aliyun.odps.mma.server.ui;

import java.util.List;

/**
 * Including all the data of a page
 */
public class PageData <T> {
  private int totalPage;
  private List<T> data;

  public PageData(int totalPage, List<T> data) {
    this.totalPage = totalPage;
    this.data = data;
  }

  public List<T> getData() {
    return data;
  }

  public int getTotalPage() {
    return totalPage;
  }
}
