export const TaskStatusMap = {
    INIT: {text: '未执行'},
    RUNNING: {text: '正在运行'},
    DONE: {text: '成功'},
    STOPPED: {text: '停止'},
    FAILED: {text: '失败'},
    SCHEMA_DOING: {text: '迁移schema中'},
    SCHEMA_DONE: {text: '迁移schema成功'},
    SCHEMA_FAILED: {text: '迁移schema失败'},
    DATA_DOING: {text: '迁移数据中'},
    DATA_DONE: {text: '迁移数据成功'},
    DATA_FAILED: {text: '迁移数据失败'},
    VERIFICATION_DOING: {text: '校验数据中'},
    VERIFICATION_DONE: {text: '校验数据成功'},
    VERIFICATION_FAILED: {text: '校验数据失败'},
} as Record<string, {text: string}>;

export const TaskStatusBadgeMap = {
    INIT: "default",
    SCHEMA_DOING: "processing",
    SCHEMA_DONE: "processing",
    SCHEMA_FAILED: "error",
    DATA_DOING: "processing",
    DATA_DONE:  "processing",
    DATA_FAILED: "error",
    VERIFICATION_DOING: "processing",
    VERIFICATION_DONE: "processing",
    VERIFICATION_FAILED: "error",
    DONE: "success"
} as Record<string, string>;