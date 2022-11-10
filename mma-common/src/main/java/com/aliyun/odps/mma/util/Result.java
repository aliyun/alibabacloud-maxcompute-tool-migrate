package com.aliyun.odps.mma.util;

import lombok.Getter;

import java.util.Objects;

public class Result<V, E> {
    @Getter
    private V value;
    @Getter
    private E error;

    private Result(V value, E error) {
        this.value = value;
        this.error = error;
    }

    public static<V, E> Result<V, E> ok(V value) {
        return new Result<V, E>(value, null);
    }

    public static<E> Result<Void, E> ok() {
        return new Result<>(null, null);
    }

    public static<V, E> Result<V, E> err(E err) {
        return new Result<>(null, err);
    }

    public boolean isOk() {
        return !isErr();
    }

    public boolean isErr() {
        return Objects.nonNull(error);
    }
}
