package com.aliyun.odps.mma.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 用于产生job id
 */
public class IdGen {
    private final AtomicInteger maxId;

    public IdGen(Integer initValue) {
        if (Objects.isNull(initValue)) {
            initValue = 0;
        }

        maxId = new AtomicInteger(initValue);
    }

    public int nextId() {
        return maxId.incrementAndGet();
    }
}
