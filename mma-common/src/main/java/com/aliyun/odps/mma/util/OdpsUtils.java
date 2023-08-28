package com.aliyun.odps.mma.util;

import com.aliyun.odps.Instance;
import com.aliyun.odps.LogView;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Task;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.config.Config;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.task.SQLTask;
import lombok.Getter;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class OdpsUtils {
    @Getter
    private final Odps odps;

    public OdpsUtils(String accessId, String accessKey, String endpoint, String defaultProject) {
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        this.odps = new Odps(account);
        this.odps.setEndpoint(endpoint);
        this.odps.setDefaultProject(defaultProject);
        String mmaFlag = MMAFlag.getMMAFlag(odps);
        this.odps.setUserAgent(mmaFlag);
    }

    public static OdpsUtils fromConfig(Config config) {
        return new OdpsUtils(
            config.getConfig(OdpsConfig.MC_AUTH_ACCESS_ID),
            config.getConfig(OdpsConfig.MC_AUTH_ACCESS_KEY),
            config.getConfig(OdpsConfig.MC_ENDPOINT),
            config.getConfig(OdpsConfig.MC_DEFAULT_PROJECT)
        );
    }

    public void setConnectTimeout(int seconds) {
        this.odps.getRestClient().setConnectTimeout(seconds);
        this.odps.getRestClient().setReadTimeout(seconds);
        this.odps.getRestClient().setRetryTimes(1);
    }

    public boolean isProjectExists(String projectName) throws OdpsException {
        return odps.projects().exists(projectName);
    }

    public String getBearerToken(String project, String tableName) throws OdpsException {
        String policy = "{\n"
                + "    \"expires_in_hours\": 168,\n"
                + "    \"policy\": {\n"
                + "        \"Statement\": [{\n"
                + "            \"Action\": [\"odps:*\"],\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Resource\": \"acs:odps:*:projects/" + project + "/tables/" + tableName
                + "\"\n"
                + "        }],\n"
                + "        \"Version\": \"1\"\n"
                + "    }\n"
                + "}";
        return odps.projects()
                .get()
                .getSecurityManager()
                .generateAuthorizationToken(policy, "Bearer");
    }

    public String getLogView(Instance i) throws OdpsException {
        return new LogView(odps).generateLogView(i, 24 * 30);
    }

    public Instance executeSql(String sql, Map<String, String> hints) throws OdpsException {
        return SQLTask.run(odps, odps.getDefaultProject(), sql, "MMAv3", hints, null);
    }

    public Instance executeTask(Task task) throws OdpsException {
        return odps.instances().create(odps.getDefaultProject(), task);
    }

    public void executeSql(String sql, Function<Instance, Void> func) throws OdpsException {
        Instance instance = SQLTask.run(odps, odps.getDefaultProject(), sql, "MMAv3", null, null);
        func.apply(instance);
    }

    public static void stop(Instance i) {
        if (Objects.nonNull(i)) {
            try {
                i.stop();
            } catch (Exception ignore) {
            }
        }
    }
}
