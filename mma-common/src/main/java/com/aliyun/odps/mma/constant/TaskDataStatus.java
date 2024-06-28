package com.aliyun.odps.mma.constant;

public class TaskDataStatus {
    public static String INIT = "DATA_INIT";

    // for merged transport
    public static String MERGE_SOURCE_DOING = "DATA_MERGE_SOURCE_DOING";
    public static String MERGE_SOURCE_FAILED = "DATE_MERGE_SOURCE_FAILED";
    public static String MERGE_SOURCE_DONE = "DATE_MERGE_SOURCE_DONE";
    public static String TRANS_DOING = "DATA_TRANS_DOING";
    public static String TRANS_FAILED = "DATA_TRANS_FAILED";
    public static String TRANS_DONE = "DATA_TRANS_DONE";
    public static String UN_MERGE_DST_DOING = "DATA_UN_MERGE_DST_DOING";
    public static String UN_MERGE_DST_FAILED = "DATA_UN_MERGE_DST_FAILED";
    public static String UN_MERGE_DST_DONE = "DATA_UN_MERGE_DST_DONE";

    // for oss transfer
    public static String OSS_TRANSFER_SRC_CREATE_EXTERNAL_TABLE = "OSS_TRANSFER_CREATE_SRC_EXTERNAL_TABLE";
    public static String OSS_TRANSFER_SRC_ADD_EXTERNAL_PTS = "";
    public static String OSS_TRANSFER_SRC_INNER_TO_EXTERNAL = "";
    public static String OSS_TRANSFER_DST_CREATE_EXTERNAL_TABLE = "";
    public static String OSS_TRANSFER_DST_ADD_EXTERNAL_PTS = "";
    public static String OSS_TRANSFER_DST_EXTERNAL_TO_INNER = "";

}
