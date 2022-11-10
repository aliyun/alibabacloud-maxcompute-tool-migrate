package com.aliyun.odps.mma.util;

import com.aliyun.odps.mma.mapper.JobMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;

@Configuration
public class UtilConfiguration {
    @Bean("JobIdGen")
    @DependsOn("DBInitializer")
    @Order()
    public IdGen getJobIdGen(@Autowired JobMapper jobMapper) {
        return new IdGen(jobMapper.maxJobId());
    }

    @Bean("TaskIdGen")
    @DependsOn("DBInitializer")
    @Order()
    public IdGen getTaskIdGen(@Autowired TaskMapper taskMapper) {
        return new IdGen(taskMapper.maxTaskId());
    }
}
