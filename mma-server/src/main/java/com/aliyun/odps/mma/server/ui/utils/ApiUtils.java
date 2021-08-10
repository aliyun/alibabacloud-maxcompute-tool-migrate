package com.aliyun.odps.mma.server.ui.utils;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.eclipse.jetty.http.MimeTypes;

import com.aliyun.odps.mma.config.ErrorOutputsV1;
import com.aliyun.odps.mma.config.OutputsWrapper;
import com.aliyun.odps.mma.util.GsonUtils;

public class ApiUtils {
  public static void handleError(HttpServletResponse resp, int sc, String message)
      throws IOException {
    resp.setStatus(sc);
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
    resp.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
    Writer writer = resp.getWriter();

    OutputsWrapper<ErrorOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    ErrorOutputsV1 outputs = new ErrorOutputsV1(message, Collections.emptyList());
    wrapper.setOutputs(outputs);
    writer.write(GsonUtils.GSON.toJson(wrapper));
  }

  public static void handleError(HttpServletResponse resp, int sc, Throwable th)
      throws IOException {
    resp.setStatus(sc);
    resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
    resp.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
    Writer writer = resp.getWriter();

    OutputsWrapper<ErrorOutputsV1> wrapper = new OutputsWrapper<>();
    wrapper.setProtocolVersion(1);
    ErrorOutputsV1 outputs = new ErrorOutputsV1(th.getMessage(), Arrays
        .asList(ExceptionUtils.getStackFrames(th)));
    wrapper.setOutputs(outputs);
    writer.write(GsonUtils.GSON.toJson(wrapper));
  }
}
