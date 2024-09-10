package com.aliyun.odps.mma.config;

import com.aliyun.odps.mma.util.StringUtils;
import com.google.gson.Gson;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Data
public class TimerConfig {
    private static final Logger logger = LoggerFactory.getLogger(TimerConfig.class);

    private static Pattern timePattern = Pattern.compile("((?<hour>\\d{2}):)?(?<minute>\\d{2})");
    private static SimpleDateFormat df = new SimpleDateFormat("HH:mm");
    private static Gson gson = new Gson();

    private TimerType type;
    private String value;

    public TimerConfig(TimerType type, String value) {
        this.type = type;
        this.value = value;
    }

    public TimerConfig() {
        this.type = TimerType.none;
    }

    public static TimerConfig parse(String json) {
        return gson.fromJson(json, TimerConfig.class);
    }

    public boolean matchTime(Calendar now) {
        if (this.type.equals(TimerType.none)) {
            return false;
        }

        int hour = now.get(Calendar.HOUR_OF_DAY);
        int minute = now.get(Calendar.MINUTE);
        Matcher matcher = timePattern.matcher(value);
        if (! matcher.matches()) {
            logger.error("invalid timer, type={}, value={}", type, value);
            return false;
        }

        int expectedMin = Integer.parseInt(matcher.group("minute"));

        switch (this.type) {
            case daily:
                int expectedHour = Integer.parseInt(matcher.group("hour"));
                return expectedHour == hour && expectedMin == minute;
            case hourly:
                return expectedMin == minute;
        }

        return false;
    }

    public static String verifyConfig(Map<String, String> config) {
        String err = verifyType(config.get("type"));
        if (! StringUtils.isBlank(err)) {
            return err;
        }

        err = verifyValue(config.get("value"));
        if (! StringUtils.isBlank(err)) {
            return err;
        }

        return "";
    }

    private static String verifyType(String type) {
        String error = "should be one of \"daily or hours\"";
        if (StringUtils.isBlank(type)) {
            return error;
        }

        try {
            TimerType.valueOf(type.toLowerCase());
        } catch (Exception e) {
            return error;
        }

        return "";
    }

    private static String verifyValue(String value) {
        String error = "invalid time";

        if (StringUtils.isBlank(value)) {
            return error;
        }

        Matcher matcher = timePattern.matcher(value);

        if (! matcher.matches()) {
            return "invalid time";
        }

        return "";
    }

    public static void main(String[] args) throws Exception {
        Calendar c = Calendar.getInstance();
        System.out.println(c.getTime().getTime());

//        while (true) {
//            Calendar c = Calendar.getInstance();
//            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime()));
//
//            TimerConfigItem timer = new TimerConfigItem(TimerType.daily, "14:19");
//            if (timer.matchTime(c)) {
//                System.out.println("ok");
//                break;
//            }
//
//            TimeUnit.SECONDS.sleep(5);
//        }
    }
}
