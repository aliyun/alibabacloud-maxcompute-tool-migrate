package com.aliyun.odps.mma.util;


import com.aliyun.odps.mma.config.TimerConfig;
import org.springframework.stereotype.Component;

@Component
public class TimerHandler extends JsonHandler<TimerConfig> {
    public TimerHandler() {
        super();
        dataType = TimerConfig.class;
    }
}
