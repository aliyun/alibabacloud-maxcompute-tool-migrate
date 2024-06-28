package com.aliyun.odps.mma.util.id;

import org.springframework.stereotype.Component;

@Component
public class DbIdGen extends ModelIdGen {
    public DbIdGen() {
        super("db", 1);
    }
}
