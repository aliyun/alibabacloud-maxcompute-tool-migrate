import {PageContainer, ProColumns, ProTable, ActionType, ProFormInstance, ProFormSelect} from "@ant-design/pro-components";
import {getJobBasicInfo, getTaskLog, getTasks, getTaskTypeMap, jobAction, taskAction} from "@/services/job";
import React, {useEffect, useRef, useState} from "react";
import {Badge, Drawer, Tabs, Popconfirm, message} from "antd";
import {PresetStatusColorType} from "antd/lib/_util/colors";
import {TaskStatusMap, TaskStatusBadgeMap} from "@/pages/Job/TaskList/components/TaskUtil";
import {TaskLog} from "@/pages/Job/TaskList/components/TaskLog";
import TaskPartitions from "@/pages/Job/TaskList/components/TaskPartitions";
import {SourceSelectorById} from "@/components/Source/SourceSelector";
import {useParams, useSearchParams} from "@@/exports";
import {McProjectSelector} from "@/components/Config/McProjectSelector";
import {useIntl} from "umi";
import {FMSpan, FM, fm} from "@/components/i18n";

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
    const [taskTypeMap, setTaskTypeMap] = useState<Record<string, string>>();
    const intl = useIntl();

    let jobId = params.get("jobId")
    let batchId = params.get("batchId")

    useEffect(() => {
        getTaskTypeMap().then((res) => {
            setTaskTypeMap(res.data ?? {});
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
            title: FM("pages.Job.TaskList.taskName", "任务名"),
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
            title: FM("pages.Job.TaskList.batchNum", "批次"),
            dataIndex: "batchId",
            valueType: "digit",
            initialValue: batchId
        },
        {
            title: FM("pages.Job.TaskList.datasource", "数据源"),
            dataIndex: "sourceName",
            key: "sourceId",
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return <SourceSelectorById />;
            }
        },
        {
            title: FM("pages.Job.TaskList.sourceDb", "源库"),
            dataIndex: 'dbName',
        },
        {
            title:  FM("pages.Job.TaskList.sourceTable", "源表"),
            dataIndex: 'tableName',
        },
        {
            title: FM("pages.Job.TaskList.dstProject", "目标项目"),
            dataIndex: 'odpsProject',
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return <McProjectSelector keyName={"odpsProject"} />;
            }
        },
        {
            title: FM("pages.Job.TaskList.dstTable", "目标表"),
            dataIndex: 'odpsTable',
        },
        {
            title: FM("pages.Job.TaskList.taskType", "类型"),
            dataIndex: 'type',
            hideInSearch: true,
            render: (row, entity) => {
                return taskTypeMap[entity.type]
            }
        },
        {
            title: FM("pages.Job.TaskList.status", "状态"),
            dataIndex: "status",
            render: (row, entity) => {
                if (entity.stopped) {
                    return (
                        <Badge
                            status="warning"
                            text={fm(intl, "pages.Job.TaskList.status.stopped", "停止")}
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
            title: FM("pages.Job.TaskList.startTime", "开始时间"),
            dataIndex: "startTime",
            search: false,
            sorter: {
                compare: (a, b) => 0,
                multiple: 1
            }
        },
        {
            title: FM("pages.Job.TaskList.endTime", "结束时间"),
            dataIndex: "endTime",
            search: false,
        },
        {
            title: FM("pages.Job.TaskList.createTime", "创建时间"),
            dataIndex: "createTime",
            search: false,
        },
        {
            title: FM("pages.Job.TaskList.partition", "分区"),
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
            title: FM("pages.Job.TaskList.operation", "操作"),
            dataIndex: 'operation',
            valueType: 'option',
            render: (row, entity) => {
                let options = [
                    <a key={"1"} onClick={() => {
                        setShowDetail(true);
                        setCurrentRow(entity);
                    }}>
                        <FMSpan id="pages.Job.TaskList.detail" defaultMessage="详情"/>
                    </a>
                ];

                if (entity.status.endsWith("_FAILED")) {
                    options.push(
                        <Popconfirm
                            key="2"
                            title={fm(intl, "pages.Job.TaskList.retryConfirm", "确定重试子任务?")}
                            onConfirm={async () => {
                                await taskAction(entity.id, "restart");
                                actionRef.current?.reload();
                            }}
                            okText="Yes"
                            cancelText="No"
                        >
                            <a href="javascript:void(0)"><FMSpan id="pages.Job.TaskList.retry" defaultMessage="重试"/></a>
                        </Popconfirm>,
                        <Popconfirm
                            key="2"
                            title={fm(intl, "pages.Job.TaskList.resetConfirm", "确定重新运行子任务?")}
                            onConfirm={async () => {
                                await taskAction(entity.id, "reset");
                                actionRef.current?.reload();
                            }}
                            okText="Yes"
                            cancelText="No"
                        >
                            <a href="javascript:void(0)"><FMSpan id="pages.Job.TaskList.reset" defaultMessage="重新运行"/></a>
                        </Popconfirm>
                    );
                }

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
                open={showDetail}
                onClose={() => {
                    setCurrentRow(undefined);
                    setShowDetail(false);
                }}
                closable={false}
            >
                {currentRow?.id && (
                    <>
                        <Tabs defaultActiveKey="1">
                            <TabPane tab={fm(intl, "pages.Job.TaskList.log", "执行日志")} key="1">
                                <TaskLog task={currentRow} />
                            </TabPane>
                            <TabPane tab={fm(intl, "pages.Job.TaskList.associated", "迁移的表/分区")} key="2">
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
