package com.aliyun.odps.mma.util.id;

import org.springframework.stereotype.Component;

@Component
public class TableIdGen extends ModelIdGen {
    public TableIdGen() {
        super("table", 2000);
    }
}
