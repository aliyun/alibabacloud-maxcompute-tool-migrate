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

package com.aliyun.odps.mma.config;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class InputsWrapper<T> extends Object {
  @Expose
  @SerializedName("ProtocolVersion")
  private Integer protocolVersion;

  @Expose
  @SerializedName("MmaSecurityEnabled")
  private boolean mmaSecurityEnabled;

  @Expose
  @SerializedName("Inputs")
  private T inputs;

  @Expose
  @SerializedName("EncryptedKey")
  private String encryptedKey;

  @Expose
  @SerializedName("EncryptedInputs")
  private String encryptedInputs;

  public void setProtocolVersion(Integer protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  public Integer getProtocolVersion() { return this.protocolVersion; }

  public void setMmaSecurityEnabled(boolean mmaSecurityEnabled) {
    this.mmaSecurityEnabled = mmaSecurityEnabled;
  }

  public boolean isMmaSecurityEnabled() { return this.mmaSecurityEnabled; }

  public void setInputs(T inputs) {
    this.inputs = inputs;
  }

  public T getInputs() { return (T)this.inputs; }

  public void setEncryptedKey(String encryptedKey) {
    this.encryptedKey = encryptedKey;
  }

  public String getEncryptedKey() { return this.encryptedKey; }

  public void setEncryptedInputs(String encryptedInputs) {
    this.encryptedInputs = encryptedInputs;
  }

  public String getEncryptedInputs() { return this.encryptedInputs; }
}
