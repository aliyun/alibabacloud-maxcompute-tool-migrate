package com.aliyun.odps.mma.constant;

public class TaskDataStatus {
    public static String INIT = "DATA_INIT";
    public static String MERGE_SOURCE_DOING = "DATA_MERGE_SOURCE_DOING";
    public static String MERGE_SOURCE_FAILED = "DATE_MERGE_SOURCE_FAILED";
    public static String MERGE_SOURCE_DONE = "DATE_MERGE_SOURCE_DONE";
    public static String TRANS_DOING = "DATA_TRANS_DOING";
    public static String TRANS_FAILED = "DATA_TRANS_FAILED";
    public static String TRANS_DONE = "DATA_TRANS_DONE";
    public static String UN_MERGE_DST_DOING = "DATA_UN_MERGE_DST_DOING";
    public static String UN_MERGE_DST_FAILED = "DATA_UN_MERGE_DST_FAILED";
    public static String UN_MERGE_DST_DONE = "DATA_UN_MERGE_DST_DONE";
}
