package com.aliyun.odps.mma.util;

import com.aliyun.odps.Odps;


public class MMAFlag {
    public static String getMMAFlag(Odps odps) {
        return "MMAv3";
        //String tenantId = OdpsTenantGetter.getTenantId(odps);
        //return String.format("MMAv3:%s", tenantId);
    }

    public static String getSQLTaskName(Odps odps) {
        return "MMAv3";
//        String tenantId = OdpsTenantGetter.getTenantId(odps);
//
//        return String.format("MMAv3_%s", tenantId);
    }
}
