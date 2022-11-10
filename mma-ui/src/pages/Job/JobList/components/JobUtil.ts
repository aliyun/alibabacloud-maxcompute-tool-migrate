export const JobStatusMap = {
    INIT: {text: '未执行'},
    DOING: {text: '执行中'},
    FAILED: {text: '失败'},
    DONE: {text: '成功'},
} as Record<string, {text: string}>;

export const JobStatusBadgeMap = {
    INIT: "default",
    DOING: "processing",
    FAILED: "error",
    DONE: "success"
} as Record<string, string>;