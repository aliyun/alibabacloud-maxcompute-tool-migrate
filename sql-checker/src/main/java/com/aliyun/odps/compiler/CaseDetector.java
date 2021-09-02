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

package com.aliyun.odps.compiler;

public class CaseDetector extends TreeScanner<Void, Void> {

  public interface Collector {
    void collect(String key, SyntaxTreeNode ast);
  }

  private Collector collector;

  public void detect(SyntaxTreeNode ast, Collector collector) {
    this.collector = collector;
    scan(ast, null);
  }

  void emit(String key, SyntaxTreeNode ast) {
    collector.collect(key, ast);
  }
}
