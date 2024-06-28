import {
    PageContainer,
    ProColumns,
    ProTable,
    ProDescriptions,
    ProDescriptionsItemProps,
    ProList, ActionType, ProFormInstance, ProFormSelect
} from "@ant-design/pro-components";
import {getJobBasicInfo, getTaskLog, getTasks, getTaskTypeMap} from "@/services/job";
import React, {useEffect, useRef, useState} from "react";
import {Badge, Drawer, Tabs, Popconfirm} from "antd";
import {PresetStatusColorType} from "antd/lib/_util/colors";
import {TaskStatusMap, TaskStatusBadgeMap} from "@/pages/Job/TaskList/components/TaskUtil";
import {TaskLog} from "@/pages/Job/TaskList/components/TaskLog";
import TaskPartitions from "@/pages/Job/TaskList/components/TaskPartitions";
import {SourceSelectorById} from "@/components/Source/SourceSelector";
import {JobSelector} from "@/components/Job/JobSelector";
import {useParams, useSearchParams} from "@@/exports";
import {McProjectSelector} from "@/components/Config/McProjectSelector";

const { TabPane } = Tabs;

export default () => {
    const [total, setTotal] = useState<number>(0);
    const [showDetail, setShowDetail] = useState<boolean>(false);
    const [currentRow, setCurrentRow] = useState<API.Task>()
    const [searchTable, setSearchTable] = useState<boolean>(false);
    const [params, _] = useSearchParams();
    const actionRef = useRef<ActionType>();
    const formRef = useRef<ProFormInstance>();
    const [jobIdToName, setJobIdToName] = useState<API.IdToName>({})

    let jobId = params.get("jobId")
    let batchId = params.get("batchId")
    let taskTypeMap: Record<string, string> = {};

    useEffect(() => {
        getTaskTypeMap().then((res) => {
            taskTypeMap = res.data ?? {};
        });

        getJobBasicInfo().then((res) => {
            const data = res?.data || {};
            setJobIdToName(data);
        })
    }, [])

    const request = async (params, sort, filter) => {
        if (params?.tableName != undefined) {
            setSearchTable(true);
        } else {
            setSearchTable(false);
        }


        if (jobId != null && params.jobId == null) {
            params.jobId = jobId;
        }

        if (batchId != null && params.batchId == null) {
            params.batchId = batchId;
        }

        let res = await getTasks(params, sort, filter);
        setTotal(res?.total || 0);
        return res;
    };

    const columns: ProColumns<API.Task>[] = [
        {
            title: "id",
            dataIndex: "id",
            search: false,
        },
        {
            title: "任务名",
            dataIndex: "jobName",
            key: "jobId",
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return (
                    <ProFormSelect
                        showSearch
                        name="jobId"
                        valueEnum={jobIdToName}
                        initialValue={jobId}
                    />
                )
            }
        },
        {
            title: "批次",
            dataIndex: "batchId",
            valueType: "digit",
            initialValue: batchId
        },
        {
            title: "数据源",
            dataIndex: "sourceName",
            key: "sourceId",
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return <SourceSelectorById />;
            }
        },
        {
            title: "源数据库",
            dataIndex: 'dbName',
        },
        {
            title: "源表",
            dataIndex: 'tableName',
        },
        {
            title: "目的项目",
            dataIndex: 'odpsProject',
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return <McProjectSelector keyName={"odpsProject"} />;
            }
        },
        {
            title: "目的表",
            dataIndex: 'odpsTable',
        },
        {
            title: "类型",
            dataIndex: 'type',
            hideInSearch: true
        },
        {
            title: "状态",
            dataIndex: "status",
            render: (row, entity) => {
                if (entity.stopped) {
                    return (
                        <Badge
                            status="warning"
                            text="停止"
                        />
                    )
                }

                return (
                    <Badge
                        status={TaskStatusBadgeMap[entity.status] as PresetStatusColorType}
                        text={TaskStatusMap[entity.status].text}
                    />
                )
            },
            valueType: 'select',
            valueEnum: TaskStatusMap,
            sorter: {
                compare: (a, b) => 0,
                multiple: 2
            }
        },
        {
            title: "开始执行时间",
            dataIndex: "startTime",
            search: false,
            sorter: {
                compare: (a, b) => 0,
                multiple: 1
            }
        },
        {
            title: "结束时间",
            dataIndex: "endTime",
            search: false,
        },
        {
            title: "创建时间",
            dataIndex: "createTime",
            search: false,
        },
        {
            title: "分区",
            key: "partition",
            hideInTable: true,
            dataIndex: "partition",
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                if (searchTable) {
                    return defaultRender(item);
                }
                return ""
            }
        },
        {
            title: "操作",
            dataIndex: 'option',
            valueType: 'option',
            render: (row, entity) => {
                let options = [
                    <a key={"1"} onClick={() => {
                        setShowDetail(true);
                        setCurrentRow(entity);
                    }}>详情</a>,
                ];

                return options;
            },
            search: false,
        }
    ]

    //setTimeout(() => formRef.current?.resetFields(), 2000);

    return (
        <PageContainer>
            <ProTable<API.Task>
                actionRef={actionRef}
                formRef={formRef}
                columns={columns}
                request={request}
                postData={(tasks: API.Task[]) => {
                    for (let task of tasks) {
                        task.type = taskTypeMap[task.type]
                    }

                    return tasks;
                }}
                rowKey="id"
                search={{
                    filterType: "light"
                }}
                pagination={{
                    showQuickJumper: true,
                    size: 'default',
                    responsive: true,
                    totalBoundaryShowSizeChanger: 10,
                    total: total
                }}
            />
            <Drawer
                width={1000}
                visible={showDetail}
                onClose={() => {
                    setCurrentRow(undefined);
                    setShowDetail(false);
                }}
                closable={false}
            >
                {currentRow?.id && (
                    <>
                        <Tabs defaultActiveKey="1">
                            <TabPane tab="执行过程" key="1">
                                <TaskLog task={currentRow} />
                            </TabPane>
                            <TabPane tab="迁移的表/分区" key="2">
                                <TaskPartitions task={currentRow} />
                            </TabPane>
                        </Tabs>
                    </>
                )}
            </Drawer>
        </PageContainer>
    );
}

const FailedStatus = ["SCHEMA_FAILED", "DATA_FAILED", "VERIFICATION_FAILED"];
