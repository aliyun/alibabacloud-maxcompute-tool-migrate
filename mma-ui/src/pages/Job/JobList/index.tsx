import services from '@/services/demo';
import {
    ActionType,
    FooterToolbar,
    PageContainer, ProColumns,
    ProDescriptions,
    ProDescriptionsItemProps,
    ProTable,
} from '@ant-design/pro-components';
import {Button, Divider, Drawer, message, Popover, Switch, Tooltip, Popconfirm, Badge, Tabs} from 'antd';
import React, { useRef, useState } from 'react';
import {getJobs, jobAction} from "@/services/job";
import {Link, useIntl} from "umi";
import {PresetStatusColorType} from "antd/lib/_util/colors";
import {McProjectSelector} from "@/components/Config/McProjectSelector";
import {SourceSelectorByName} from "@/components/Source/SourceSelector";
import {JobDetail} from "@/pages/Job/JobList/components/JobDetail";
import {FM, fm, FMSpan} from "@/components/i18n"

export default () => {
    const [showJobDetail, setShowJobDetail] = useState<boolean>(false);
    const actionRef = useRef<ActionType>();
    const [currentRow, setCurrentRow] = useState<API.Job>();
    const intl = useIntl();


    const JobStatusMap = {
        INIT: {text: fm(intl, "pages.Job.type.status.init", "未执行")},
        DOING: {text: fm(intl, "pages.Job.type.status.doing", "执行中")},
        FAILED: {text: fm(intl, "pages.Job.type.status.failed", "失败")},
        DONE: {text: fm(intl, "pages.Job.type.status.done", "成功")},
    } as Record<string, {text: string}>;

    const JobStatusBadgeMap = {
        INIT: "default",
        DOING: "processing",
        FAILED: "error",
        DONE: "success"
    } as Record<string, string>;

    const columns: ProColumns<API.Job>[] = [
         {
             title: FM("pages.Job.taskName", "任务名"),
             dataIndex: 'description',
             render: (e, j) => {
                 return <Link to={`/jobs/tasks?jobId=${j.id}`}>{j.description}</Link>
             }
         },
         {
             title: FM("pages.Job.datasource", "数据源"),
             dataIndex: 'source_name',
             key: "sourceName",
             renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                 return <SourceSelectorByName />;
             }
         },
         {
             title: FM("pages.Job.sourceDb", "源数据库"),
             dataIndex: 'db_name',
             key: "dbName",
         },
        {
            title: FM("pages.Job.dstProject", "目的项目"),
            dataIndex: 'dst_mc_project',
            key: "dstOdpsProject",
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return <McProjectSelector keyName={"dstOdpsProject"} />;
            }
        },
         {
             title: FM( "pages.Job.type", "类型"),
             dataIndex: 'type',
             valueType: 'select',
             valueEnum: {
                 Tables: {text: fm(intl,"pages.Job.type.multiTables", "多表")},
                 Database: {text: fm(intl,"pages.Job.type.multiTables", "单库")},
                 Partitions: {text: fm(intl,"pages.Job.type.multiTables", "多分区")},
             }
         },
         // {
         //     title: "其他信息",
         //     dataIndex: "config",
         //     colSize: 1,
         //     //valueType: 'code',
         //     renderText: (text: any, record, index: number, action: ProCoreActionType) => {
         //         return JSON.stringify(record.config, null, 4);
         //     },
         //     //@ts-ignore
         //     ellipsis: {
         //         showTitle: false,
         //     },
         //     render: (row, entity) => {
         //         let config = JSON.stringify(entity.config, null, 4);
         //         //return config;
         //
         //         return (
         //             <Tooltip placement="topLeft" title={config}>
         //                 {config}
         //             </Tooltip>
         //         )
         //     },
         //     search: false,
         // },
         {
             title: FM("pages.Job.type.status", "状态"),
             dataIndex: "status",
             render: (row, entity) => {
                 if (entity.stopped) {
                     return (
                         <Badge
                             status="warning"
                             text={fm(intl, "pages.Job.type.status.stopped", "停止")}
                         />
                     )
                 }

                 return (
                     <Badge
                         status={JobStatusBadgeMap[entity.status] as PresetStatusColorType}
                         text={JobStatusMap[entity.status].text}
                     />
                 )
             },
             valueType: 'select',
             valueEnum: {
                 INIT: {text: fm(intl, "pages.Job.type.status.init", "未执行")},
                 DOING: {text: fm(intl, "pages.Job.type.status.doing", "执行中")},
                 DONE: {text:  fm(intl, "pages.Job.type.status.done", "完成")},
                 FAILED: {text: fm(intl, "pages.Job.type.status.failed", "失败")},
                 STOPPED: {text: fm(intl, "pages.Job.type.status.stopped", "停止")},
             },
         },
        {
            title: FM("pages.Job.timer", "定时"),
            dataIndex: "timer",
            search: false,
            render: (row, entity) => {
                const timer = entity.timer;
                if (! timer) {
                    return "--";
                }

                let content = "";
                switch (timer.type) {
                    case "daily":
                        content = `${fm(intl, "pages.Job.timer.daily", "每天")}: ${timer.value}`
                        break;
                    case "hourly":
                        content = `${fm(intl, "pages.Job.timer.hourly", "每小时")}: ${timer.value}`
                        break
                    default:
                        return "--";
                }

                return (
                    <a onClick={() => {
                        setShowJobDetail(true);
                        setCurrentRow(entity);
                    }}>
                        {content}
                    </a>
                );
            }
        },
         {
             title: FM("pages.Job.createTime", "每天"),
             dataIndex: "createTime",
             search: false,
             //sorter: (a, b) => (a.createTime - b.createTime),

         },
         {
             title: FM("pages.Job.operation", "操作"),
             dataIndex: 'operation',
             valueType: 'option',
             render: (row, entity) => {
                 let options = [];

                 if (entity.status == "DOING") {
                     options.push(
                         <Popconfirm
                             key="1"
                             title={
                                 entity.stopped ?
                                     fm(intl, "pages.Job.operation.start", "开始任务") :
                                     fm(intl, "pages.Job.operation.stop", "停止任务")
                             }
                             onConfirm={async () => {
                                 if (! entity.stopped) {
                                     let hide = message.loading(fm(intl, "pages.Job.operation.stopping", "停止任务"))
                                     await jobAction(entity.id, "stop");
                                     hide();
                                     message.success(fm(intl, "pages.Job.operation.stopOk", "停止成功"));
                                 } else {
                                     await jobAction(entity.id, "start");
                                     message.success(fm(intl, "pages.Job.operation.startOk", "启动成功"));
                                 }

                                 actionRef.current?.reload();
                             }}
                         >
                             <a type="primary">
                                 {
                                     entity.stopped ?
                                         fm(intl, "pages.Job.operation.start", "启动") :
                                         fm(intl, "pages.Job.operation.stop", "停止")
                                 }
                             </a>
                         </Popconfirm>
                     )
                 }

                 options.push([
                     <Popconfirm
                         key="2"
                         title={fm(intl, "pages.Job.deletionConfirm", "确定删除任务?")}
                         onConfirm={async () => {
                             let hide = message.loading(fm(intl, "pages.Job.deletingMsg", "停止并删除任务中..."))
                             await jobAction(entity.id, "delete");
                             hide();
                             actionRef.current?.reload();
                         }}
                         okText="Yes"
                         cancelText="No"
                     >
                         <a href="#"><FMSpan id="pages.Job.delete" defaultMessage="删除" /></a>
                     </Popconfirm>
                 ]);

                 if (entity.status == "FAILED") {
                     options.push(
                         <Popconfirm
                             key={"3"}
                             title={fm(intl, "pages.Job.retry.confirm", "确定重试任务?")}
                             onConfirm={async () => {
                                 await jobAction(entity.id, "retry");
                                 setTimeout(() => {
                                     actionRef.current?.reload();
                                 }, 2000);

                             }}
                             okText="Yes"
                             cancelText="No"
                         >
                             <a href="#"><FMSpan id="pages.Job.retry" defaultMessage="重试" /></a>
                         </Popconfirm>
                     )
                 }

                 return options;
             },
             search: false,
         }

     ]

     return  (
         <PageContainer>
             <ProTable<API.Job>
                 actionRef={actionRef}
                 columns={columns}
                 request={(params, sort, filter) => {
                     return getJobs(params, sort, filter)
                 }}
                 rowKey="id"
                 search={{
                     filterType: "light"
                 }}
             />

             <Drawer
                 width={"40%"}
                 open={showJobDetail}
                 onOpenChange={setShowJobDetail}
                 onClose={() => {
                     setCurrentRow(undefined);
                     setShowJobDetail(false);
                 }}
                 closable={false}
             >
                 <Tabs>
                     <Tabs.TabPane tab={fm(intl, "pages.Job.jobBatches", "执行批次")} key="job_batch">
                         <JobDetail jobId={currentRow?.id} />
                     </Tabs.TabPane>
                 </Tabs>
             </Drawer>
         </PageContainer>
     )
 }

