package com.aliyun.odps.mma.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;

import java.util.*;

public class ApiRes {
    @JsonProperty("message")
    @Setter
    private String message;
    @JsonProperty("success")
    private boolean success;
    @JsonProperty("errors")
    private Map<String, String> errors;
    private Map<String, Object> data;

    @JsonAnyGetter
    public Map<String, Object> getData() {
        return data;
    }

    public ApiRes() {}

    public ApiRes(String message) {
        this.message = message;
    }

    public ApiRes(String message, Map<String, String> errors) {
        this.message = message;
        this.errors = errors;
        this.success = true;
    }

    public static ApiRes ok() {
        ApiRes apiRes = new ApiRes("ok");
        apiRes.success = true;
        apiRes.data = new HashMap<>();
        return apiRes;
    }

    public static ApiRes ok(Map<String, Object> data) {
        ApiRes apiRes = new ApiRes("ok", null);
        apiRes.data = data;

        return apiRes;
    }

    public static ApiRes ok(String key, Object value) {
        ApiRes apiRes = new ApiRes("ok", null);
        apiRes.data = new HashMap<>(1);
        apiRes.data.put(key, value);

        return apiRes;
    }

    public static ApiRes error(String message, Map<String, String> errors) {
        ApiRes apiRes = new ApiRes(message, errors);
        apiRes.success = false;
        return apiRes;
    }

    public static ApiRes error(String message, String key, String errorMsg) {
        ApiRes apiRes = new ApiRes(message);
        apiRes.success = false;
        apiRes.addError(key, errorMsg);

        return apiRes;
    }

    public void addError(String key, String msg) {
        if (Objects.isNull(this.errors)) {
            this.errors = new HashMap<>();
        }

        this.errors.put(key, msg);
    }

    public void addData(String key, Object value) {
        this.data.put(key, value);
    }

    public String toJson() {
        ObjectMapper om = new ObjectMapper();
        try {
            return om.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<ApiRes> tryFromJson(String json) {
        ObjectMapper om = new ObjectMapper();
        try {
            return Optional.of(om.readValue(json, ApiRes.class));
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    @JsonIgnore
    public boolean isOk() {
        return success;
    }
}
