import services from '@/services/demo';
import {
    ActionType,
    FooterToolbar,
    PageContainer, ProColumns,
    ProDescriptions,
    ProDescriptionsItemProps,
    ProTable,
} from '@ant-design/pro-components';
import {Button, Divider, Drawer, message, Popover, Switch, Tooltip, Popconfirm, Badge} from 'antd';
import React, { useRef, useState } from 'react';
import {getJobs, jobAction} from "@/services/job";
import {ProCoreActionType} from "@ant-design/pro-utils/lib/typing";
import {Link} from "umi";
import {JobStatusBadgeMap, JobStatusMap} from "@/pages/Job/JobList/components/JobUtil";
import {PresetStatusColorType} from "antd/lib/_util/colors";
import {McProjectSelector} from "@/components/Config/McProjectSelector";
import {SourceSelectorByName} from "@/components/Source/SourceSelector";

export default () => {
    const actionRef = useRef<ActionType>();

    const columns: ProColumns<API.Job>[] = [
         {
             title: "任务名",
             dataIndex: 'description',
             render: (e, j) => {
                 return <Link to={`/jobs/tasks?jobId=${j.id}`}>{j.description}</Link>
             }
         },
         {
             title: "数据源",
             dataIndex: 'source_name',
             key: "sourceName",
             renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                 return <SourceSelectorByName />;
             }
         },
         {
             title: "源数据库",
             dataIndex: 'db_name',
             key: "dbName",
         },
        {
            title: "目的项目",
            dataIndex: 'dst_mc_project',
            key: "dstOdpsProject",
            renderFormItem: (item, { type, defaultRender, ...rest }, form) => {
                return <McProjectSelector keyName={"dstOdpsProject"} />;
            }
        },
         {
             title: "类型",
             dataIndex: 'type',
             valueType: 'select',
             valueEnum: {
                 Tables: {text: "多表"},
                 Database: {text: "单库"},
                 Partitions: {text: "多分区"},
             }
         },
         {
             title: "其他信息",
             dataIndex: "config",
             colSize: 1,
             //valueType: 'code',
             renderText: (text: any, record, index: number, action: ProCoreActionType) => {
                 return JSON.stringify(record.config, null, 4);
             },
             //@ts-ignore
             ellipsis: {
                 showTitle: false,
             },
             render: (row, entity) => {
                 let config = JSON.stringify(entity.config, null, 4);
                 //return config;

                 return (
                     <Tooltip placement="topLeft" title={config}>
                         {config}
                     </Tooltip>
                 )
             },
             search: false,
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
                         status={JobStatusBadgeMap[entity.status] as PresetStatusColorType}
                         text={JobStatusMap[entity.status].text}
                     />
                 )
             },
             valueType: 'select',
             valueEnum: {
                 INIT: {text: '未执行'},
                 DOING: {text: '执行中'},
                 DONE: {text: '完成'},
                 FAILED: {text: '失败'},
                 STOPPED: {text: '停止'},
             },
         },
         {
             title: "创建时间",
             dataIndex: "createTime",
             search: false,
             //sorter: (a, b) => (a.createTime - b.createTime),

         },
         {
             title: "操作",
             dataIndex: 'option',
             valueType: 'option',
             render: (row, entity) => {
                 let options = [];

                 if (entity.status == "DOING") {
                     options.push(
                         <Popconfirm
                             key="1"
                             title={entity.stopped ? "开启任务" : "停止任务"}
                             onConfirm={async () => {
                                 if (! entity.stopped) {
                                     let hide = message.loading("停止任务中...")
                                     await jobAction(entity.id, "stop");
                                     hide();
                                     message.success("停止任务成功");
                                 } else {
                                     await jobAction(entity.id, "retry");
                                     message.success("启动任务成功");
                                 }

                                 actionRef.current?.reload();
                             }}
                         >
                             <a type="primary">{entity.stopped ? "启动" : "停止"}</a>
                         </Popconfirm>
                     )
                 }

                 options.push([
                     <Popconfirm
                         key="2"
                         title="确定删除任务?"
                         onConfirm={async () => {
                             let hide = message.loading("停止并删除任务中...")
                             await jobAction(entity.id, "delete");
                             hide();
                             actionRef.current?.reload();
                         }}
                         okText="Yes"
                         cancelText="No"
                     >
                         <a href="#">删除</a>
                     </Popconfirm>
                 ]);

                 if (entity.status == "FAILED") {
                     options.push(
                         <Popconfirm
                             key={"3"}
                             title="确定重试任务?"
                             onConfirm={async () => {
                                 await jobAction(entity.id, "retry");
                                 setTimeout(() => {
                                     actionRef.current?.reload();
                                 }, 2000);

                             }}
                             okText="Yes"
                             cancelText="No"
                         >
                             <a href="#">重试</a>
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
                 // toolbar={{
                 //     actions: [
                 //         <Button
                 //             key="key"
                 //             type="primary"
                 //             onClick={() => {
                 //                 alert('add');
                 //             }}
                 //         >
                 //             新建
                 //         </Button>,
                 //     ]
                 // }}
             />
         </PageContainer>
     )
 }

