package com.aliyun.odps.mma.scheduling;

import com.aliyun.odps.mma.config.TimerConfig;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.orm.JobProxy;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.service.JobService;
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
public class JobTimer {
    private static final Logger logger = LoggerFactory.getLogger(JobTimer.class);
    private final ConcurrentMap<Integer, Long> cache = new ConcurrentHashMap<>();
    @Autowired
    private JobService js;
    @Autowired
    private OrmFactory ormFactory;

    @Scheduled(fixedRate = 10000)
    public void run() {
        // 清理cache, 将缓存60s以上的缓存项清除
        Calendar now = Calendar.getInstance();
        cleanCache(now);

        // 获取有定时器的job
        List<JobModel> jobs = js.listJobsWithTimer();

        // 检查是否触发定时
        List<JobModel> jobsTriggered = new ArrayList<>();
        for (JobModel job: jobs) {
            // 60s内，已经被执行过的数据源不被执行
            if (cache.containsKey(job.getId())) {
                continue;
            }

            TimerConfig timerConfig = job.getTimer();
            if (timerConfig.matchTime(now)) {
                jobsTriggered.add(job);
                logger.info("trigger job {}", job.getId());
                cache.put(job.getId(), now.getTime().getTime());
            }
        }

        for (JobModel job: jobsTriggered) {
            JobProxy jobProxy = ormFactory.newJobProxy(job);
            try {
                jobProxy.submit();
            } catch (Exception e) {
                logger.error("failed to submit job on timer", e);
            }
        }
    }

    private void cleanCache(Calendar now) {
        Long nowTs = now.getTime().getTime();
        for (Integer key: cache.keySet()) {
            Long ts = cache.get(key);

            if (nowTs - ts > 60*1000L) {
                cache.remove(key);
            }
        }
    }
}
