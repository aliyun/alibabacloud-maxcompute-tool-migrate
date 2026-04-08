package com.aliyun.odps.mma.scheduling;

import com.aliyun.odps.mma.config.TimerConfig;
import com.aliyun.odps.mma.meta.DSManager;
import com.aliyun.odps.mma.model.ConfigItem;
import com.aliyun.odps.mma.service.ConfigService;
import com.aliyun.odps.mma.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class MetaLoadingTimer {
    private static final Logger logger = LoggerFactory.getLogger(MetaLoadingTimer.class);
    private final ConcurrentMap<String, Long> cache = new ConcurrentHashMap<>();

    @Autowired
    private ConfigService cs;

    @Autowired
    private DSManager dsManager;

    @Scheduled(fixedRate = 10000)
    public void run() {
        // 清理cache, 将缓存60s以上的缓存项清除
        Calendar now = Calendar.getInstance();
        cleanCache(now);

        // 获取数据源配置里的timer配置
        List<ConfigItem> timers = cs.getTimers();

        // 查看当前时间是否满足timer, 如果满足则更新数据源
        List<String> sources = new ArrayList<>();

        for (ConfigItem config: timers) {
            String source = config.getCategory();

            // 60s内，已经被执行过的数据源不被执行
            if (cache.containsKey(source)) {
                continue;
            }

            String timerValue = config.getValue();

            if (StringUtils.isBlank(timerValue)) {
                continue;
            }

            TimerConfig timer = TimerConfig.parse(timerValue);
            if (timer.matchTime(now)) {
                sources.add(source);
                cache.put(source, now.getTime().getTime());
            }
        }

        for (String source: sources) {
            // 每个数据源会起一个线程来进行元数据更细
            dsManager.loadDataSourceMeta(source);
        }
    }

    private void cleanCache(Calendar now) {
        Long nowTs = now.getTime().getTime();
        for (String key: cache.keySet()) {
            Long ts = cache.get(key);

            if (nowTs - ts > 60*1000L) {
                cache.remove(key);
            }
        }
    }
}
