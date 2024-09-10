import {useEffect, useState} from "react";
import {getTaskPartitions} from "@/services/job";
import {message, Descriptions, Badge } from "antd";
import {useIntl} from "umi";
import {fm} from "@/components/i18n";

export default (props: {task: API.Task}) => {
    const [ptList, setPtList] = useState<API.PartitionModel[]>([]);
    const intl = useIntl();

    useEffect(()=> {
        getTaskPartitions(props.task.id)
            .then((res) => {
                setPtList(res?.data || []);
            })
            .catch(() => {
                message.error( fm(intl, "pages.Job.TaskList.TaskPartitions.failedMsg", "加载子任务分区失败"), 5);
            });
    }, [props])

    let task = props.task;
    let ptElements = ptList.map((pt) => {
        return <>{pt.value} <br /> </>
    });

    return (
        <Descriptions title="User Info" bordered column={2}>
            <Descriptions.Item label={fm(intl, "pages.Job.TaskList.TaskPartitions.datasource", "数据源")}>{task.sourceName}</Descriptions.Item>
            <Descriptions.Item label={fm(intl, "pages.Job.TaskList.TaskPartitions.sourceDb", "源数据库")}>{task.dbName}</Descriptions.Item>
            <Descriptions.Item label={fm(intl, "pages.Job.TaskList.TaskPartitions.sourceTable", "源表")}>{task.tableName}</Descriptions.Item>
            <Descriptions.Item label={fm(intl, "pages.Job.TaskList.TaskPartitions.dstProject", "目的项目")}>{task.odpsProject}</Descriptions.Item>
            <Descriptions.Item label={fm(intl, "pages.Job.TaskList.TaskPartitions.dstTable", "目的表")}>{task.odpsTable}</Descriptions.Item>
            <Descriptions.Item label={fm(intl, "pages.Job.TaskList.TaskPartitions.partitions", "分区")}>
                {ptElements}
            </Descriptions.Item>
        </Descriptions>
    )
}