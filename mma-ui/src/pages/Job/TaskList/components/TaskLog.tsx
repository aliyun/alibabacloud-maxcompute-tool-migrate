import React, {useEffect, useState} from "react";
import {downloadTaskLog, getTaskLog} from "@/services/job";
import {message, Timeline, Row, Col, Badge, Button} from "antd";
import {TaskStatusBadgeMap, TaskStatusMap} from "@/pages/Job/TaskList/components/TaskUtil";
import {PresetStatusColorType} from "antd/lib/_util/colors";
import {getLocale, useIntl} from "umi";
import {FMSpan, FM, fm} from "@/components/i18n";

export const TaskLog = (props: {task: API.Task}) => {
    const [taskLogs, setTaskLogs] = useState<API.TaskLog[]>([]);
    const intl = useIntl();

    const logGetter = () => {
        getTaskLog(props.task.id)
            .then((res) => {
                setTaskLogs(res.data || [])
            })
            .catch(() => {
                message.error(fm(intl, "pages.Job.TaskList.TaskLog", "加载子任务log失败"), 5);
            });
    }

    useEffect(() => {
        logGetter();
        let itId = setInterval(logGetter, 2000);

        return function cleanup() {
            clearInterval(itId);
        }
    }, [props]);

    let items = taskLogs.map((log) => {
        return(
            <Timeline.Item key={log.id} color={TaskStatusColorMap[log.status]}>
                <Row gutter={[0, 5]}>
                    <Col span={1}><FMSpan id="pages.Job.TaskList.TaskLog.time" defaultMessage="时间:" /></Col>
                    <Col>{log.createTime}</Col>
                </Row>
                <Row>
                    <Col span={2}><FMSpan id="pages.Job.TaskList.TaskLog.status" defaultMessage="状态:" /></Col>
                    <Col>
                        <Badge
                            status={TaskStatusBadgeMap[log.status] as PresetStatusColorType}
                            text={TaskStatusMap[log.status].text}
                        />
                    </Col>
                </Row>
                <Row>
                    <Col span={2}><FMSpan id="pages.Job.TaskList.TaskLog.action" defaultMessage="动作:" /></Col>
                    <Col>{(()=>{
                        if (log?.action.startsWith("http")) {
                            return <a href={log.action}>{log.action}</a>
                        }

                        return log?.action || "--";
                    })()}</Col>
                </Row>
                <Row>
                    <Col span={2}><FMSpan id="pages.Job.TaskList.TaskLog.result" defaultMessage="结果:" /></Col>
                    <Col>{(()=>{
                        if (! log){
                            return "--";
                        }

                        return log.msg.split("$")
                            .flatMap((p) => p.split("\n"))
                            .map((p) => {
                                if (p.startsWith("http")) {
                                    return <a href={p} target="_blank">{p}</a>;
                                }

                                return  <p>{p}</p>;
                            })
                    })()}</Col>
                </Row>
            </Timeline.Item>
        )
    });

    return (
        <Row justify={items.length > 0 ? "end": ""}>
            <Col>
                {items.length > 0 ? <a href={`/api/tasks/${props.task.id}/logs?lang=${getLocale()}`}><FMSpan id="pages.Job.TaskList.TaskLog.downloadLog" defaultMessage="下载日志"/></a> : ""}
            </Col>

            <Col span={24}>
                <Timeline>
                    {items.length > 0 ? items : <span><FMSpan id="pages.Job.TaskList.TaskLog.noLog" defaultMessage="无执行日志"/></span>}
                </Timeline>
            </Col>
        </Row>
    )
};

let TaskStatusColorMap = {
    INIT: "gray",
    SCHEMA_DOING: "green",
    SCHEMA_DONE: "green",
    SCHEMA_FAILED: "red",
    DATA_DOING: "green",
    DATA_DONE:  "green",
    DATA_FAILED: "red",
    VERIFICATION_DOING: "green",
    VERIFICATION_DONE: "green",
    VERIFICATION_FAILED: "red",
    DONE: "green"
} as Record<string, string>;