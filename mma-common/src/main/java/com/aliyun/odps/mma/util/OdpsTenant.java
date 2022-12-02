package com.aliyun.odps.mma.util;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class OdpsTenant {
    private String ownerId;
    private String tenantId;
    private final RestClient restClient;

    public OdpsTenant(Odps odps) {
        this.restClient = odps.getRestClient();
    }

    public void load() throws OdpsException {
        Response res = this.restClient.request("/tenants","GET", null, null, null);
        String json = new String(res.getBody(), StandardCharsets.UTF_8);
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        ResModel resModel = gson.fromJson(json, ResModel.class);

        this.tenantId = resModel.Tenant.TenantId;
        this.ownerId = resModel.Tenant.OwnerId;
    }

    public String getTenantId() throws Exception {
        if (Objects.isNull(tenantId)) {
            load();
        }

        return tenantId;
    }

    public String getOwnerId() throws Exception {
        if (Objects.isNull(ownerId)) {
            load();
        }

        return ownerId;
    }

    static class ResModel {
        public TenantModel Tenant;
    }

    static class TenantModel {
        public String TenantId;
        public String OwnerId;
    }
}
