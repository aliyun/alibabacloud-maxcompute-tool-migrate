package com.aliyun.odps.mma;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@MapperScan("com.aliyun.odps.mma.mapper")
@EnableScheduling
@EnableAsync
public class MMAServer {
    public static void main(String[] args) {
        CmdArgParser parser = new CmdArgParser();
        parser.parse(args);

        SpringApplicationBuilder builder = new SpringApplicationBuilder(MMAServer.class);
        SpringApplication app = builder.build();
        app.setBannerMode(Banner.Mode.OFF);
        app.run(args);
    }
}
