package com.aliyun.odps.mma.util.id;

public interface IdGenerator {
    Long nextId(String name) throws IdGenException;
}
