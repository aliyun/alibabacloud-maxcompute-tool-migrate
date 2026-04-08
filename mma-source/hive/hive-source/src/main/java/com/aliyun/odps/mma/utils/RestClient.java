package com.aliyun.odps.mma.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

public class RestClient {
    private static final Logger logger = LoggerFactory.getLogger(RestClient.class);
    int READ_BUFFER_SIZE = 1024;

    public String get(String urlStr, Map<String, String> params) throws RestException {
        return request("GET", urlStr, params, null);
    }

    public String request(String method, String urlStr, Map<String, String> params, byte[] body) throws RestException {
        return request(method, urlStr, params, body, "application/json",false);
    }

    public String request(String method, String urlStr, Map<String, String> params, byte[] body, String contentType, boolean useZip) throws RestException {
        try {
            URL url = new URL(buildUrl(urlStr, params));

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            try {
                conn.setRequestMethod(method);
                initConn(conn);

                if (Objects.nonNull(body)) {
                    conn.setDoOutput(true);

                    if (Objects.nonNull(contentType)) {
                        conn.setRequestProperty("Content-Type", contentType);
                    }

//                    if (useZip) {
//                        conn.setRequestProperty("Content-Encoding", "gzip");
//                        conn.setRequestProperty("Transfer-Encoding", "gzip, chunked");
//                        conn.setChunkedStreamingMode(1500 - 4);
//                    }

                    try(OutputStream out = conn.getOutputStream()) {
                        InputStream bodyStream = new ByteArrayInputStream(body);

                        int bufferSize = READ_BUFFER_SIZE;
                        if (body.length < READ_BUFFER_SIZE) {
                            bufferSize = body.length;
                        }

                        byte[] buffer = new byte[bufferSize];

                        if (useZip) {
                            GZIPOutputStream zo = new GZIPOutputStream(out);
                            copy(bodyStream, zo, buffer);
                            zo.close();
                        } else {
                            copy(bodyStream, out, buffer);
                        }

                        copy(bodyStream, out, buffer);
                    }
                }

                String res = getRes(conn);

                if (conn.getResponseCode() != 200) {
                    throw RestException.Error(urlStr, conn.getResponseCode(), res);
                }

                return res;
            } finally {
                conn.disconnect();
            }
        } catch (Exception e) {
            throw new RestException(e);
        }
    }

    private byte[] getBuffer(HttpURLConnection conn) {
        Map<String, List<String>> headers = conn.getHeaderFields();
        List<String> values = headers.get("Content-Length");

        if (Objects.nonNull(values) && values.size() > 0) {
            String valStr = values.get(0);

            try {
                int length = Integer.parseInt(valStr);
                if (length > READ_BUFFER_SIZE) {
                    return new byte[READ_BUFFER_SIZE];
                }

                return new byte[length];
            } catch (NumberFormatException e) {
                // ignore
            }
        }

        return new byte[READ_BUFFER_SIZE];
    }

    private void initConn(HttpURLConnection conn) {
        conn.setConnectTimeout(0);
        conn.setReadTimeout(0);
    }


    private String getRes(HttpURLConnection conn) throws Exception {
        byte[] buffer = getBuffer(conn);
        if (buffer.length == 0) {
            return "";
        }

        InputStream is = conn.getInputStream();
        ByteArrayOutputStream bs = new ByteArrayOutputStream();
        copy(is, bs, buffer);
        is.close();

        return new String(bs.toByteArray(),  Charset.forName(StandardCharsets.UTF_8.name()));
    }

    private int copy(InputStream src, OutputStream dst, byte[] buffer) throws Exception {
        int count = 0;
        int n = 0;
        while ((n = src.read(buffer)) != -1) {
            dst.write(buffer, 0, n);
            count += n;
        }

        return count;
    }

    private String buildUrl(String url, Map<String, String> params) {
        if (Objects.isNull(params) || params.isEmpty()) {
            return url;
        }

        return String.format("%s?%s", url, encodeUrlParams(params));
    }

    private String encodeUrlParams(Map<String, String> params) {
        return params
                .entrySet()
                .stream()
                .map((entry) -> String.format("%s=%s", entry.getKey(), encode(entry.getValue())))
                .collect(Collectors.joining("&"));
    }

    private String encode(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }

        String r = null;
        try {
            r = URLEncoder.encode(str, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Encode failed: " + str);
        }
        r = r.replaceAll("\\+", "%20");
        return r;
    }
}
