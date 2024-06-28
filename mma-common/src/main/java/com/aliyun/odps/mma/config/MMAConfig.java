package com.aliyun.odps.mma.config;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MMAConfig extends Config {
    @ConfigItem(desc = "maxcompute endpoint", required = true)
    public static String MC_ENDPOINT = "mc.endpoint";
    @ConfigItem(desc = "用于数据传输的maxcompute endpoint")
    public static String MC_DATA_ENDPOINT = "mc.data.endpoint";
    @ConfigItem(desc = "maxcompute tunnel endpoint")
    public static String MC_TUNNEL_ENDPOINT = "mc.tunnel.endpoint";
    @ConfigItem(desc = "maxcompute access id", required = true)
    public static String MC_AUTH_ACCESS_ID = "mc.auth.access.id";
    @ConfigItem(desc = "maxcompute access key", required = true, type = "password")
    public static String MC_AUTH_ACCESS_KEY = "mc.auth.access.key";
    @ConfigItem(desc = "maxcompute default project", required = true)
    public static String MC_DEFAULT_PROJECT = "mc.default.project";
    @ConfigItem(desc = "maxcompute tunnel quota")
    public static String MC_TUNNEL_QUOTA = "mc.tunnel.quota";
    @ConfigItem(desc = "要迁往的maxcompute项目列表", type = "list")
    public static String MC_PROJECTS = "mc.projects";
    @ConfigItem(desc = "数据搬迁任务最大并发量", type = "int", defaultValue = "20")
    public static String TASK_MAX_NUM = "task.max.num";
    @ConfigItem(desc = "UDTF鉴权方式:BearerToken、AK", defaultValue = "BearerToken", enums = {"BearerToken", "AK"})
    public static String AUTH_TYPE = "auth.type";
    @ConfigItem(desc = "UDTF鉴权方式=AK 时，config.ini 在 HDFS 上的路径", defaultValue = "hdfs:///tmp/odps_config.ini")
    public static String AUTH_AK_HDFS_PATH = "auth.ak.hdfs.path";

    public MMAConfig() {
        super();
    }

    public String category() {
        return "common";
    }

    public String getMcDataEndpoint() {
        return this.getOrDefault(
                MMAConfig.MC_DATA_ENDPOINT,
                this.getConfig(MMAConfig.MC_ENDPOINT)
        );
    }

    public List<String> getDstMcProjects() {
        List<String> projects =  this.getList(MC_PROJECTS);

        if (projects.size() == 0) {
            projects.add(this.getConfig(MC_DEFAULT_PROJECT));
        }

        return projects;
    }

    public void setInited() {
        setBoolean("inited", true);
    }

    public boolean hasInited() {
        return getBoolean("inited");
    }
}
