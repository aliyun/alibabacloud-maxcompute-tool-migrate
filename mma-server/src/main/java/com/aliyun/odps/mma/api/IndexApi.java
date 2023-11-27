package com.aliyun.odps.mma.api;

import com.aliyun.odps.mma.MMAServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api")
public class IndexApi {
    ConfigurableApplicationContext appCtx;

    @Value("${mma-version}")
    String mmaVersion;
    @Autowired
    public IndexApi(ConfigurableApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    @PutMapping("/restart")
    public void restart() {
        Thread thread = new Thread(() -> {
            ApplicationArguments args = appCtx.getBean(ApplicationArguments.class);
            appCtx.close();
            SpringApplicationBuilder builder = new SpringApplicationBuilder(MMAServer.class);
            SpringApplication app = builder.build();
            app.setBannerMode(Banner.Mode.OFF);
            app.run(args.getSourceArgs());
        });

        thread.setDaemon(false);
        thread.start();
    }

    @GetMapping("/ping")
    public String ping() {
        return "ok";
    }

    @GetMapping("/version")
    public String getVersion() {
        return mmaVersion;
    }
}
