package com.aliyun.odps.mma.util;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.mma.config.Config;
import com.aliyun.odps.mma.config.OdpsConfig;
import com.aliyun.odps.mma.orm.TaskProxy;
import com.aliyun.odps.task.SQLTask;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class OdpsUtils {
    @Getter
    private final Odps odps;

    public OdpsUtils(String accessId, String accessKey, String endpoint, String defaultProject, String logviewHost) {
        AliyunAccount account = new AliyunAccount(accessId, accessKey);
        this.odps = new Odps(account);
        this.odps.setEndpoint(endpoint);
        this.odps.setDefaultProject(defaultProject);
        String mmaFlag = MMAFlag.getMMAFlag(odps);
        this.odps.setUserAgent(mmaFlag);
        this.odps.getRestClient().setRetryTimes(3);
        this.odps.getRestClient().setConnectTimeout(30);
        this.odps.getRestClient().setReadTimeout(30);
        this.odps.getRestClient().setRetryStrategy(new MMARetryStrategy(null, 5));
        if (!StringUtils.isBlank(logviewHost)) {
            this.odps.setLogViewHost(logviewHost);
        }
    }

    public void setRetry(TaskProxy task) {
        this.odps.getRestClient().setRetryStrategy(new MMARetryStrategy(task, 5));
    }

    public static OdpsUtils fromConfig(Config config) {
        return new OdpsUtils(
            config.getConfig(OdpsConfig.MC_AUTH_ACCESS_ID),
            config.getConfig(OdpsConfig.MC_AUTH_ACCESS_KEY),
            config.getConfig(OdpsConfig.MC_ENDPOINT),
            config.getConfig(OdpsConfig.MC_DEFAULT_PROJECT),
            config.getConfig(OdpsConfig.LOG_VIEW_HOST)
        );
    }

    public void setConnectTimeout(int seconds) {
        this.odps.getRestClient().setConnectTimeout(seconds);
        this.odps.getRestClient().setReadTimeout(seconds);
        this.odps.getRestClient().setRetryTimes(2);
    }

    public Project getProject(String projectName) throws OdpsException {
        return odps.projects().get(projectName);
    }

    public boolean isProjectExists(String projectName) throws OdpsException {
        return odps.projects().exists(projectName);
    }

    public String getBearerToken(String project, String schema, String tableName) throws OdpsException {
        String schemaPath = "";
        if (!StringUtils.isBlank(schema)) {
            schemaPath = "/schemas/" + schema;
        }

        String policy = "{\n"
                + "    \"expires_in_hours\": 24,\n"
                + "    \"policy\": {\n"
                + "        \"Statement\": [{\n"
                + "            \"Action\": [\"odps:*\"],\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Resource\": \"acs:odps:*:projects/" + project + schemaPath + "/tables/" + tableName
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

    public String getSuperBearerToken(String project, String schema, String tableName) throws OdpsException {
        String schemaPath = "";
        if (!StringUtils.isBlank(schema)) {
            schemaPath = "/schemas/" + schema;
        }

        String policy = "{\n"
                + "    \"expires_in_hours\": 24,\n"
                + "    \"policy\": {\n"
                + "        \"Statement\": ["
                + "          {\n"
                + "            \"Action\": [\"odps:*\"],\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Resource\": \"acs:odps:*:projects/" + project + schemaPath + "/tables/" + tableName + "\"\n"
                + "          },\n"
                + "          {\n"
                + "            \"Action\": [\"odps:*\"],\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Resource\": \"acs:odps:*:projects/" + project + "\"\n"
                + "          }\n" +
                "           ],\n"
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

    public List<String> listSchemas(String projectName) {
        Iterable<Schema> schemaIter = odps.schemas().iterable(projectName);
        List<String> names = new ArrayList<>();

        for (Schema schema: schemaIter) {
            names.add(schema.getName());
        }

        return names;
    }
}
