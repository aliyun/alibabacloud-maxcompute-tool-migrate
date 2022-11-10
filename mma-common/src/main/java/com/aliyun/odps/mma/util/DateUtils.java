package com.aliyun.odps.mma.util;

import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {
    public static Date fromSeconds(long seconds) {
        return new Date(seconds * 1000);
    }

    public static String formattedDate(Date date) {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));

        return sf.format(date);
    }
}
