package com.aliyun.odps.mma.api;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.mma.config.MMAConfig;
import com.aliyun.odps.mma.meta.MetaLoaderUtils;
import com.aliyun.odps.mma.util.OdpsUtils;
import com.aliyun.odps.mma.validator.JsonField;
import com.aliyun.odps.mma.validator.ValidateJson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Validated
@RestController
@RequestMapping("/api/config")
public class ConfigApi {
    private final MMAConfig config;
    private final MetaLoaderUtils metaLoaderUtil;

    @Autowired
    public ConfigApi(MMAConfig config, MetaLoaderUtils metaLoaderUtil) {
        this.config = config;
        this.metaLoaderUtil = metaLoaderUtil;
    }

    @Value("${spring.profiles.active:prod}")
    private String activeProfile;

    @GetMapping("")
    public ApiRes getAllConfig() {
        return ApiRes.ok("data", config.toJsonObj());
    }

    @GetMapping("/status")
    public ApiRes getStatus() {
        Map<String, Boolean> status = new HashMap<>();
        status.put("inited", config.hasInited());
        return ApiRes.ok("data", status);
    }

    @PostMapping("")
    public ApiRes initConfig(
            @ValidateJson(
                    message = "failed to init config",
                    fields = {
                            @JsonField(key="mc.endpoint", required = true),
                            @JsonField(key="mc.auth.access.id", required = true),
                            @JsonField(key="mc.auth.access.key", required = true),
                            @JsonField(key="mc.default.project", required = true),
                    }
            )
            @RequestBody Map<String, Object> configItems
    ) {
        ApiRes apiRes = updateConfig(configItems);

        if (apiRes.isOk()) {
            config.setInited();
        }

        return apiRes;
    }

    @PutMapping("")
    public ApiRes updateConfig(
            @ValidateJson(message = "failed to update config")
            @RequestBody Map<String, Object> configItems
    ) {
        try {
            config.openMemMode();
            Map<String, String> errors = config.addConfigItems(configItems);
            if (errors.size() > 0) {
                return ApiRes.error("config error", errors);
            }

            OdpsUtils odpsUtils = OdpsUtils.fromConfig(config);
            odpsUtils.setConnectTimeout(2);

            try {
                boolean ok = odpsUtils.isProjectExists(config.getConfig(MMAConfig.MC_DEFAULT_PROJECT));

                if (! ok) {
                    return ApiRes.error("config error", "mc.default.project", "does not exist");
                }

                config.dumpMem();
            } catch (Exception e) {
                String errMsg = e.getMessage();
                if (errMsg.contains("connect timed out")) {
                    return ApiRes.error("config error", "mc.endpoint", errMsg);
                }

                if (errMsg.contains("accessKeyId not found")) {
                    return ApiRes.error("config error", "mc.auth.access.id", errMsg);
                }

                if (errMsg.contains("User signature dose not match")) {
                    return ApiRes.error("config error", "mc.auth.access.key", errMsg);
                }

                if (errMsg.contains("URI")) {
                    return ApiRes.error("config error", "mc.endpoint", errMsg);
                }

                return ApiRes.error("config error", "unknown", e.getMessage());
            }


            return ApiRes.ok();
        } finally {
            config.closeMemMode();
        }
    }

    @GetMapping("/dstMcProjects")
    public ApiRes getDstMcProjects() {
        List<String> dstMcProjects = config.getDstMcProjects();
        return ApiRes.ok("data", dstMcProjects);
    }
}
