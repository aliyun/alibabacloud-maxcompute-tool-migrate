package com.aliyun.odps.mma.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class KeyLock implements AutoCloseable {
    static final ConcurrentMap<String, Boolean> map = new ConcurrentHashMap<>();

    private String key;

    public KeyLock(String key) {
        this.key = key;
    }

    public void lock() throws InterruptedException {
        while (true) {
            if (map.containsKey(key)) {
                TimeUnit.MILLISECONDS.sleep(10);
                continue;
            }

            AtomicBoolean ok = new AtomicBoolean(false);
            map.computeIfAbsent(key, (k) -> {
                ok.set(true);
                return true;
            });

            if (ok.get()) {
                break;
            }
        }
    }

    @Override
    public void close() throws Exception {
        map.remove(key);
    }
}
