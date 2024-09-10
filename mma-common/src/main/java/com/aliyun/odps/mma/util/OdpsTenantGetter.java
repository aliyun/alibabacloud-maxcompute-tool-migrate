package com.aliyun.odps.mma.util;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.BearerTokenAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OdpsTenantGetter {
    private static final Logger logger = LoggerFactory.getLogger(OdpsTenantGetter.class);
    static ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();

    public static String getTenantId(Odps odps) {
        Account account = odps.getAccount();
        String hashStr;

        if (account instanceof AliyunAccount) {
            AliyunAccount aa = (AliyunAccount) account;
            hashStr = String.format("%s.%s",aa.getAccessId(), aa.getAccessKey());
        } else if (account instanceof BearerTokenAccount) {
            BearerTokenAccount ba = (BearerTokenAccount) account;
            hashStr = ba.getToken();
        } else {
            throw new RuntimeException("mma can only use aliyun account or bear token account now.");
        }

        String tenantId = cache.get(hashStr);

        if (Objects.nonNull(tenantId)) {
            return tenantId;
        }

        OdpsTenant tenant = new OdpsTenant(odps);
        try {
            tenant.load();
            tenantId = tenant.getTenantId();
            putValue(hashStr, tenantId);
            return tenantId;
        } catch (Exception e) {
            logger.error("", e);
        }

        return "";
    }

    public static synchronized void putValue(String key, String value){
        cache.put(key, value);
    }
}
