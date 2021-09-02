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

package com.aliyun.odps.mma.client;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;

import com.aliyun.odps.mma.config.InputsWrapper;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.JobInfoOutputsV1;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.config.PublicKeyOutputsV1;
import com.aliyun.odps.mma.util.Constants;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class MmaClient {

  private static final String JOB_API = "/api/jobs";

  private boolean isMmaSecurityEnabled;
  private PublicKey publicKey;
  private KeyGenerator aesKeyGenerator;
  private HttpClient httpClient;

  public MmaClient(String host, int port) throws Exception {
    this.httpClient = new HttpClient(host, port);
    aesKeyGenerator = KeyGenerator.getInstance("AES");
    aesKeyGenerator.init(128);
    init();
  }

  private void init() throws Exception {
    String responseBody = httpClient.get("/api/publickey", null);
    Type t = new TypeToken<OutputsWrapper<PublicKeyOutputsV1>>() {}.getType();
    OutputsWrapper<PublicKeyOutputsV1> wrapper = GsonUtils.GSON.fromJson(responseBody, t);

    isMmaSecurityEnabled = wrapper.getOutputs().isMmaSecurityEnabled();
    if (isMmaSecurityEnabled) {
      byte[] key = Base64.decodeBase64(wrapper.getOutputs().getPublicKey());
      // Init public key & cipher
      X509EncodedKeySpec keySpec = new X509EncodedKeySpec(key);
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      publicKey = keyFactory.generatePublic(keySpec);
    }
  }

  private <T> byte[] getBody(T inputs)
      throws NoSuchPaddingException,
      NoSuchAlgorithmException,
      InvalidKeyException,
      BadPaddingException,
      IllegalBlockSizeException {

    if (isMmaSecurityEnabled) {
      SecretKey aesKey = aesKeyGenerator.generateKey();
      Cipher rsaCipher = Cipher.getInstance("RSA");
      rsaCipher.init(Cipher.PUBLIC_KEY, publicKey);
      String encryptedAesKey = Base64.encodeBase64String(rsaCipher.doFinal(aesKey.getEncoded()));

      Cipher aesCipher = Cipher.getInstance("AES");
      aesCipher.init(Cipher.ENCRYPT_MODE, aesKey);
      String encryptedInputs = Base64.encodeBase64String(aesCipher.doFinal(inputs.toString().getBytes()));

      InputsWrapper inputsWrapper = new InputsWrapper();
      inputsWrapper.setProtocolVersion(1);
      inputsWrapper.setMmaSecurityEnabled(isMmaSecurityEnabled);
      inputsWrapper.setEncryptedKey(encryptedAesKey);
      inputsWrapper.setEncryptedInputs(encryptedInputs);

      return GsonUtils.GSON.toJson(inputsWrapper).getBytes(StandardCharsets.UTF_8);
    } else {
      InputsWrapper<T> inputsWrapper = new InputsWrapper<>();
      inputsWrapper.setProtocolVersion(1);
      inputsWrapper.setMmaSecurityEnabled(isMmaSecurityEnabled);
      inputsWrapper.setInputs(inputs);
      return GsonUtils.GSON.toJson(inputsWrapper).getBytes(StandardCharsets.UTF_8);
    }
  }

  public void submitJob(JobConfiguration config) throws Exception {
    httpClient.post("/api/jobs", null, getBody(config));
  }

  public List<JobInfoOutputsV1> listJobs() throws Exception {
    String response = httpClient.get(JOB_API, null);
    OutputsWrapper<List<JobInfoOutputsV1>> outputsWrapper = GsonUtils.GSON.fromJson(
        response,
        new TypeToken<OutputsWrapper<List<JobInfoOutputsV1>>>() {}.getType());
    return outputsWrapper.getOutputs();
  }

  public JobInfoOutputsV1 getJobInfo(String jobId) throws Exception {
    String response = httpClient.get(
        JOB_API,
        Collections.singletonMap(Constants.JOB_ID_PARAM, jobId));
    OutputsWrapper<JobInfoOutputsV1> outputsWrapper = GsonUtils.GSON.fromJson(
        response,
        new TypeToken<OutputsWrapper<JobInfoOutputsV1>>() {}.getType());
    return outputsWrapper.getOutputs();
  }

  public void stopJob(String jobId) throws Exception {
    httpClient.delete(JOB_API, Collections.singletonMap(Constants.JOB_ID_PARAM, jobId));
  }

  public void deleteJob(String jobId) throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put(Constants.JOB_ID_PARAM, jobId);
    params.put(Constants.PERMANENT_PARAM, "true");
    httpClient.delete(JOB_API, params);
  }

  public void resetJob(String jobId) throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put(Constants.JOB_ID_PARAM, jobId);
    params.put(Constants.ACTION_PARAM, Constants.RESET_ACTION);
    httpClient.put(JOB_API, params, null);
  }

  public void updateJob(String jobId, JobConfiguration config) throws Exception {

  }
}
