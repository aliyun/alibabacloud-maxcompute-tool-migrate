package com.aliyun.odps.mma.util;

public class StringUtils {
    public static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    public static String trim(String origin, String pattern) {
        if (origin.startsWith(pattern)) {
            origin = origin.substring(pattern.length());
        }

        if (origin.endsWith(pattern)) {
            origin = origin.substring(0, origin.length() - pattern.length());
        }

        return origin;
    }

    public static void main(String[] args) {
        System.out.println(StringUtils.trim("'hello'", "'"));
    }
}
