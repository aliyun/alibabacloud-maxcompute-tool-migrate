package com.aliyun.odps.mma.util.id;

import org.springframework.stereotype.Component;

@Component
public class JobIdGen extends ModelIdGen {
    public JobIdGen() {
        super("job", 1);
    }
}
