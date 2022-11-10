package com.aliyun.odps.mma;

import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.config.HiveConfig;
import com.aliyun.odps.mma.mapper.JobMapper;
import com.aliyun.odps.mma.mapper.PartitionMapper;
import com.aliyun.odps.mma.mapper.TableMapper;
import com.aliyun.odps.mma.mapper.TaskMapper;
import com.aliyun.odps.mma.meta.*;
import com.aliyun.odps.mma.model.JobModel;
import com.aliyun.odps.mma.model.PartitionModel;
import com.aliyun.odps.mma.orm.OrmFactory;
import com.aliyun.odps.mma.query.JobFilter;
import com.aliyun.odps.mma.service.DbService;
import com.aliyun.odps.mma.service.TableService;
import com.aliyun.odps.mma.service.TaskService;
import com.aliyun.odps.mma.task.TaskManager;
import com.aliyun.odps.mma.task.TaskUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


@SpringBootApplication
public class MMATester implements CommandLineRunner {
    @Autowired
    DataSourceMetaLoader dw;

    @Autowired
    MMAConfig config;

    @Autowired
    HiveConfig hiveConfig;

    @Autowired
    HiveMetaLoader loader;

    @Autowired
    DbService dbService;

    @Autowired
    TableService tableService;

    @Autowired
    TaskService taskService;

    @Autowired
    TaskMapper taskMapper;

    @Autowired
    JobMapper jobMapper;

    @Autowired
    PartitionMapper partitionMapper;

    @Autowired
    ApplicationContext appCtx;

    @Autowired
    TaskUtils taskUtil;

    @Autowired
    TaskManager taskManager;

    @Autowired
    TableMapper tableMapper;

    @Autowired
    OrmFactory proxyFactory;

    @Autowired
    DSManager dsManager;

    @Autowired
    PartitionMapper pm;

    public static void main(String[] args) {
        CmdArgParser parser = new CmdArgParser();
        parser.parse(args);

        SpringApplicationBuilder builder = new SpringApplicationBuilder(MMATester.class);
        builder.web(WebApplicationType.NONE);
        SpringApplication app = builder.build();

        ConfigurableApplicationContext ctx = app.run(args);

        if (ctx.isRunning()) {
            SpringApplication.exit(ctx, () -> 0);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        JobModel jobModel = jobMapper.getJobById(1);
        JobFilter jf = new JobFilter();
        ObjectMapper om = new ObjectMapper();
        List<JobModel> jobs = jobMapper.getJobs(jf);
        System.out.println(om.writeValueAsString(jobs));
    }
}
