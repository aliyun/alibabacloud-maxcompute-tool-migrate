package com.aliyun.odps.mma.client;

import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;
import java.util.stream.Collectors;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.aliyun.odps.mma.config.InputsWrapper;
import com.aliyun.odps.mma.config.JobConfiguration;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.config.PublicKeyOutputsV1;
import com.aliyun.odps.mma.util.GsonUtils;
import com.google.gson.reflect.TypeToken;

public class HttpClient {

  private static final Logger LOG = LogManager.getLogger(HttpClient.class);

  private String host;
  private int port;

  public HttpClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  private String request(
      String method,
      String api,
      Map<String, String> params, byte[] inputs) throws Exception {

    LOG.info("Method: {}, api: {}, params: {}", method, api, params);

    String uri = String.format("http://%s:%d%s", host, port, api);
    if (params != null && !params.isEmpty()) {
      uri += String.format("?%s", String.join("&", params
          .entrySet()
          .stream()
          .map(e -> e.getKey() + "=" + e.getValue())
          .collect(Collectors.toList())));
    }

    HttpURLConnection connection = (HttpURLConnection) new URI(uri).toURL().openConnection();
    try {
      connection.setRequestMethod(method);
      connection.setDoInput(true);
      connection.setDoOutput(true);

      connection.setReadTimeout(600 * 1000);
      connection.setConnectTimeout(10 * 1000);

      if (("POST".equals(method) || "PUT".equals(method)) && inputs != null && inputs.length != 0) {
        connection.setRequestProperty("Content-Length", Integer.toString(inputs.length));
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "*/*");

        connection.setFixedLengthStreamingMode(inputs.length);
        try (OutputStream os = connection.getOutputStream()) {
          os.write(inputs);
          os.flush();
        }
      }

      if (100 <= connection.getResponseCode() && connection.getResponseCode() <= 399) {
        return new String(
            IOUtils.toByteArray(connection.getInputStream()),
            StandardCharsets.UTF_8);
      } else {
        String errMsg = new String(
            IOUtils.toByteArray(connection.getErrorStream()),
            StandardCharsets.UTF_8);
        if (StringUtils.isBlank(errMsg)) {
          errMsg = new String(
              IOUtils.toByteArray(connection.getInputStream()),
              StandardCharsets.UTF_8);
        }
        throw new Exception(String.format("[%d] %s", connection.getResponseCode(), errMsg));
      }
    } finally {
      connection.disconnect();
    }
  }

  String get(String api, Map<String, String> params) throws Exception {
    return request("GET", api, params, null);
  }

  String post(String api, Map<String, String> params, byte[] inputs) throws Exception {
    return request("POST", api, params, inputs);
  }

  String delete(String api, Map<String, String> params) throws Exception {
    return request("DELETE", api, params, null);
  }

  String put(String api, Map<String, String> params, byte[] inputs) throws Exception {
    return request("PUT", api, params, inputs);
  }
}
