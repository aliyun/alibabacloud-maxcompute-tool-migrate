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

package com.aliyun.odps.datacarrier.taskscheduler.ui.api;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.datacarrier.taskscheduler.MmaException;
import com.aliyun.odps.datacarrier.taskscheduler.MmaServerConfig;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public abstract class AbstractRestfulApi {
  private static final Logger LOG = LogManager.getLogger(AbstractRestfulApi.class);

  static final Gson GSON = (new GsonBuilder()).create();

  PublicKey publicKey;
  PrivateKey privateKey;
  private String prefix;
  private boolean securityEnabled;

  public AbstractRestfulApi(String prefix) throws MmaException {
    Map<String, String> apiConfig = MmaServerConfig.getInstance().getApiConfig();
    this.securityEnabled = Boolean.valueOf(apiConfig.get("mma.api.security.enabled"));

    if (this.securityEnabled) {
      String publicKeyPath = apiConfig.get("mma.api.security.publicKey.path");
      String privateKeyPath = apiConfig.get("mma.api.security.privateKey.path");
      if (StringUtils.isNullOrEmpty(publicKeyPath) || StringUtils.isNullOrEmpty(privateKeyPath)) {
        throw new MmaException("Failed to init MMA API. Public/private keys are required");
      }

      try {
        KeyFactory kf = KeyFactory.getInstance("RSA");
        byte[] privateKeyBytes = Files.readAllBytes((new File(privateKeyPath)).toPath());
        PKCS8EncodedKeySpec pri = new PKCS8EncodedKeySpec(privateKeyBytes);
        this.privateKey = kf.generatePrivate(pri);

        byte[] publicKeyBytes = Files.readAllBytes((new File(publicKeyPath)).toPath());
        X509EncodedKeySpec pub = new X509EncodedKeySpec(publicKeyBytes);
        this.publicKey = kf.generatePublic(pub);
      } catch (NoSuchAlgorithmException|IOException|java.security.spec.InvalidKeySpecException e) {
        throw new MmaException("Failed to init MMA API. Failed to load public/private keys", e);
      }
    }

    this.prefix = prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public String getPrefix() {
    return this.prefix;
  }

  public String handleGet(HttpServletRequest request) throws ServletException, IOException {
    throw new ServletException("GET is not supported");
  }

  public String handlePost(HttpServletRequest request) throws ServletException, IOException {
    throw new ServletException("POST is not supported");
  }

  public String handleDelete(HttpServletRequest request) throws ServletException, IOException {
    throw new ServletException("DELETE is not supported");
  }

  boolean isSecurityEnabled() {
    return this.securityEnabled;
  }

  JsonElement getDecryptedInputs(JsonObject body)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
    boolean isEncrypted = false;
    if (body.has("MmaSecurityEnabled")) {
      isEncrypted = body.getAsJsonPrimitive("MmaSecurityEnabled").getAsBoolean();
    }

    if (isEncrypted) {
      String encryptedKey = body.get("EncryptedKey").getAsString();
      String encryptedInputs = body.get("EncryptedInputs").getAsString();

      Cipher rsaCipher = Cipher.getInstance("RSA");
      rsaCipher.init(2, this.privateKey);
      byte[] decryptedKey = rsaCipher.doFinal(Base64.decodeBase64(encryptedKey));

      SecretKey aesKey = new SecretKeySpec(decryptedKey, "AES");
      Cipher aesCipher = Cipher.getInstance("AES");
      aesCipher.init(2, aesKey);
      byte[] decryptedInputsBytes = aesCipher.doFinal(Base64.decodeBase64(encryptedInputs));
      JsonParser jsonParser = new JsonParser();
      return jsonParser.parse(new String(decryptedInputsBytes, StandardCharsets.UTF_8));
    }
    return body.get("Inputs").getAsJsonObject();
  }
}
