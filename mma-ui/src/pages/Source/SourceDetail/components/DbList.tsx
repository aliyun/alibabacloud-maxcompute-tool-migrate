import {ProColumns, ProTable, ProDescriptions} from "@ant-design/pro-components";
import type { ActionType, ProDescriptionsItemProps } from '@ant-design/pro-components';
import {Card, Descriptions, Drawer, Switch} from "antd";
import {getDbs, getPts} from "@/services/source";
import {formatSize} from "@/utils/format";
import {Link} from "umi";
import React, {useState} from "react";
import {NewJobForm} from "@/components/Job/NewJobForm";
import {SOURCES_ROUTE} from "@/constant";

export default (props?: { source?: API.DataSource }) => {
    const ds = props?.source;
    const [currentDb, setCurrentDb] = useState<API.DbModel|null>(null);
    const [visible, setVisible] = useState<boolean>(false);
    const [total, setTotal] = useState<number>(0);

    const columns: ProColumns<API.DbModel>[] = [
        {
            title: "数据源",
            dataIndex: "sourceName",
            hideInTable: ds?.id != undefined,
            hideInSearch: ds?.id != undefined,
        },
        {
            title: "库名",
            dataIndex: 'name',
            render: (dom, entity) => {
                return (<Link to={`${SOURCES_ROUTE}/${ds?.name}/${entity.name}?sourceId=${ds?.id}&dbId=${entity.id}`}>{entity.name}</Link>);
            }
        },
        {
            title: "table总数",
            search: false,
            dataIndex: "tables",
        },
        {
            title: "迁移完成table数",
            search: false,
            dataIndex: "tablesDone",
        },
        {
            title: "有错误的table数",
            search: false,
            dataIndex: "tablesFailed",
            hideInTable: true
        },
        {
            title: "部分完成table数",
            search: false,
            dataIndex: "tablesPartDone",
            hideInTable: true
        },
        {
            title: "正在迁移table数",
            search: false,
            dataIndex: "tablesDoing",
            hideInTable: true
        },
        {
            title: "分区总数",
            search: false,
            dataIndex: 'partitions',
        },
        {
            title: "迁移完成分区数",
            search: false,
            dataIndex: 'partitionsDone',
        },
        {
            title: "出错的分区数",
            search: false,
            dataIndex: "partitionsFailed",
            hideInTable: true
        },
        {
            title: "正在迁移分区数",
            search: false,
            dataIndex: "partitionsDoing",
            hideInTable: true
        },
        {
            title: "大小",
            dataIndex: 'size',
            render: (_, entity) => {
                if (entity.size === undefined || entity.size === 0) {
                    return "--"
                } else {
                    return formatSize(entity.size);
                }
            },
            search: false,
            sorter: {
                compare: (a, b) => 0,
                multiple: 1
            }
        },
        {
            title: "行数",
            dataIndex: 'numRows',
            render: (_, entity) => {
                if (entity.numRows === undefined || entity.numRows === 0) {
                    return "--"
                } else {
                    return entity.numRows;
                }
            },
            search: false,
            sorter: {
                compare: (a, b) => 0,
                multiple: 2
            }
        },
        {
            title: "状态",
            dataIndex: 'status',
            valueType: 'select',
            valueEnum: {
                INIT: {text: '未迁移'},
                DONE: {text: '完成'},
                DOING: {text: '迁移中'},
                FAILED: {text: '部分完成，有失败'},
                PART_DONE: {text: '部分完成'},
            },
            filters: true,
        },
        {
            title: "操作",
            render: (row, entity) => {
                return [
                    <a key={1} onClick={() => {
                        setVisible(true);
                        setCurrentDb(entity);
                    }}>迁移</a>
                ]
            },
            search: false,
        }
    ];

    return (
        <>
            <ProTable<API.DbModel>
                columns={columns}
                request={async (params, sort, filter) => {
                    params["sourceId"] = ds?.id;
                    let res = await getDbs(params, sort, filter);
                    setTotal(res?.total || 0);
                    return res;
                }}
                rowKey="id"
                pagination={{
                    showQuickJumper: true,
                    size: 'default',
                    responsive: true,
                    totalBoundaryShowSizeChanger: 2,
                    total: total
                }}
                search={{
                    filterType: 'light'
                }}
                tableExtraRender={(_, data) => {
                    if (ds == undefined) {
                        return <></>;
                    }

                    return (
                        <Card>
                            <Descriptions size="small" column={3}>
                                <Descriptions.Item key={1} label="数据库">{ds.dbNum}</Descriptions.Item>
                                <Descriptions.Item key={2}  label="表">{ds.tableNum}</Descriptions.Item>
                                <Descriptions.Item key={3}  label="分区">{ds.partitionNum}</Descriptions.Item>
                                <Descriptions.Item key={4}  label="最新更新时间">{ds.lastUpdateTime}</Descriptions.Item>
                            </Descriptions>
                        </Card>
                    );
                }}
            />

            <NewJobForm db={currentDb} jobType="database" visible={visible} setVisible={setVisible} />
        </>
    );
}
