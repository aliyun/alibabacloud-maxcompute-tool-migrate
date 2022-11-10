package com.aliyun.odps.mma.constant;

public enum TaskStatus {
    INIT,
    SCHEMA_DOING,
    SCHEMA_DONE,
    SCHEMA_FAILED,
    DATA_DOING,
    DATA_DONE,
    DATA_FAILED,
    VERIFICATION_DOING,
    VERIFICATION_DONE,
    VERIFICATION_FAILED,
    DONE;

    public static TaskStatus getFailStatus(TaskStatus status) {
        switch (status) {
            case INIT:
            case SCHEMA_DOING:
                return SCHEMA_FAILED;
            case SCHEMA_DONE:
            case DATA_DOING:
                return DATA_FAILED;
            case DATA_DONE:
            case VERIFICATION_DOING:
                return VERIFICATION_FAILED;
            default:
                return status;
        }
    }
}
