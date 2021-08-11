package com.aliyun.odps.mma;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

public class TestUtils {

  public static final String MC_ENDPOINT = "mc_endpoint";
  public static final String MC_DEFAULT_PROJECT = "mc_default_project";
  public static final String MC_ACCESSKEY_ID = "mc_accesskey_id";
  public static final String MC_ACCESSKEY_SECRET = "mc_accesskey_secret";

  public static final String HIVE_JDBC_URL = "hive_jdbc_url";
  public static final String HIVE_JDBC_USERNAME = "hive_jdbc_username";
  public static final String HIVE_JDBC_PASSWORD = "hive_jdbc_password";

  public static final String HIVE_METASTORE_URIS = "hive_metastore_uris";

  public static final String OSS_ENDPOINT = "oss_endpoint";
  public static final String OSS_BUCKET = "oss_bucket";
  public static final String OSS_ACCESSKEY_ID = "oss_accesskey_id";
  public static final String OSS_ACCESSKEY_SECRET = "oss_accesskey_secret";
  public static final String OSS_PATH = "oss_path";

  private static Properties properties = null;

  private static void loadProperties() throws IOException {
    if (properties == null) {
      properties = new Properties();
      InputStream is = Thread
          .currentThread()
          .getContextClassLoader()
          .getResourceAsStream("test.properties");
      properties.load(is);
    }
  }

  public static void printProperties() throws IOException {
    loadProperties();
    for (String propertyName: properties.stringPropertyNames()) {
      String value = properties.getProperty(propertyName);
      System.err.println(String.format("%s=%s", propertyName, value));
    }
  }

  public static String getProperty(String key) throws IOException {
    loadProperties();
    return properties.getProperty(key);
  }
}
