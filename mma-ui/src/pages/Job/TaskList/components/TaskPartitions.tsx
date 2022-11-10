import {useEffect, useState} from "react";
import {getTaskPartitions} from "@/services/job";
import {message, Descriptions, Badge } from "antd";

export default (props: {task: API.Task}) => {
    let [ptList, setPtList] = useState<API.PartitionModel[]>([]);

    useEffect(()=> {
        getTaskPartitions(props.task.id)
            .then((res) => {
                setPtList(res?.data || []);
            })
            .catch(() => {
                message.error("加载子任务分区失败", 5);
            });
    }, [props])

    let task = props.task;
    let ptElements = ptList.map((pt) => {
        return <>{pt.value} <br /> </>
    });

    return (
        <Descriptions title="User Info" bordered column={2}>
            <Descriptions.Item label="数据源">{task.sourceName}</Descriptions.Item>
            <Descriptions.Item label="源数据库">{task.dbName}</Descriptions.Item>
            <Descriptions.Item label="源表">{task.tableName}</Descriptions.Item>
            <Descriptions.Item label="目的项目">{task.odpsProject}</Descriptions.Item>
            <Descriptions.Item label="目的表">{task.odpsTable}</Descriptions.Item>
            <Descriptions.Item label="分区">
                {ptElements}
            </Descriptions.Item>
        </Descriptions>
    )
}