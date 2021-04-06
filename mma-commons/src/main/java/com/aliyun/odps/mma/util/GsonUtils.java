package com.aliyun.odps.mma.util;

import java.lang.reflect.Modifier;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonUtils {
  public static final Gson GSON = new GsonBuilder()
      .excludeFieldsWithModifiers(Modifier.STATIC, Modifier.VOLATILE)
      .disableHtmlEscaping()
      .setPrettyPrinting()
      .create();
}
