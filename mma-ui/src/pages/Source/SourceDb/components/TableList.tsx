import {ProColumns, ProTable, ProDescriptions, ProDescriptionsItemProps} from "@ant-design/pro-components";
import {formatSize} from "@/utils/format";
import {getDbs, getTables} from "@/services/source";
import React, {useState} from "react";
import {StatusMap} from "@/pages/Source/SourceDb/components/Status";
import {Button, Col, Descriptions, Drawer, Row, Table, Tabs} from "antd";
import {NewJobForm} from "@/components/Job/NewJobForm";
import {Key, RowSelectMethod} from "antd/lib/table/interface";
import {useIntl} from "umi";
import {FMSpan, FM, fm} from "@/components/i18n";

export default (props: {db: API.DbModel}) => {
    const [total, setTotal] = useState<number>(0);
    const [newJobFormOpen, setNewJobFormOpen] = useState<boolean>(false);
    const [tables, setTables] = useState<string[]>([]);
    const [showDetail, setShowDetail] = useState<boolean>(false);
    const [currentRow, setCurrentRow] = useState<API.TableModel>()
    const intl = useIntl();

    const columns: ProColumns<API.TableModel>[] = [
        {
            title: "Schema",
            dataIndex: "schemaName",
            // formItemProps: {
            //     rules: [
            //         {
            //             required: true,
            //             message: '表名为必填项',
            //         },
            //     ],
            // }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.name", "表名"),
            dataIndex: "name",
            // formItemProps: {
            //     rules: [
            //         {
            //             required: true,
            //             message: '表名为必填项',
            //         },
            //     ],
            // }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.partitionTable", "分区表"),
            dataIndex: "hasPartitions",
            render: (_, entity) => {
                return entity.hasPartitions ? "yes": "no";
            },
            valueEnum: {
                "no": {text: "非分区表"},
                "yes": {text: "分区表"}
            },
            valueType: "select",
            //@ts-ignore
            sorter: (a, b) => (a.hasPartitions && 1) - (b.hasPartitions && 1),
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.type", "类型"),
            dataIndex: "type",
            valueEnum: {
                "MANAGED_TABLE" : {text: "managed table"},
                "VIRTUAL_VIEW" : {text: "virtual view"},
                "EXTERNAL_TABLE" : {text: "external table"},
                "MATERIALIZED_VIEW": {text: "materialized view"},
            },
            valueType: "select",
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.partitions", "分区"),
            dataIndex: "partitions",
            hideInSearch: true,
            sorter: (a, b) => a.partitions - b.partitions,
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.ptCopied", "完成分区"),
            dataIndex: "partitionsDone",
            hideInSearch: true
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.ptCopying", "正在迁移分区"),
            dataIndex: "partitionsDoing",
            hideInSearch: true
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.ptFailed", "失败的分区"),
            dataIndex: "partitionsFailed",
            hideInSearch: true
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.size", "大小"),
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
                compare: (a, b) => a.size - b.size,
                multiple: 2
            }
        },

        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.rows", "行数"),
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
                compare: (a, b) => a.size - b.size,
                multiple: 1
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.changed", "元数据有更新"),
            dataIndex: "updated",
            render: (_, entity) => {
                return entity.updated ? "yes": "no";
            },
            valueType: "select",
            valueEnum: {
                0: {text: 'no'},
                1: {text: 'yes'},
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.lastModifiedTime", "数据最后修改时间"),
            render: (_, entity) => {
                return entity.lastDdlTime;
            },
            dataIndex: "lastDdlTime",
            valueType: 'dateRange',
            sorter: {
                compare: (a, b) => 0,
                multiple: 1
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.status", "状态"),
            dataIndex: 'status',
            valueType: 'checkbox',
            valueEnum: {
                INIT: {text: fm(intl, "pages.Source.SourceDb.components.TableList.statusInit", "未迁移")},
                DONE: {text: fm(intl, "pages.Source.SourceDb.components.TableList.statusDone", "完成")},
                DOING: {text: fm(intl, "pages.Source.SourceDb.components.TableList.statusDoing", "迁移中")},
                FAILED: {text: fm(intl, "pages.Source.SourceDb.components.TableList.statusFailed", "部分完成，有失败")},
                PART_DONE: {text: fm(intl, "pages.Source.SourceDb.components.TableList.statusPartDone", "部分完成")},
            },
            sorter: (a, b) => (StatusMap[a.status] -  StatusMap[b.status]),
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.tableSchema", "Schema"),
            dataIndex: "name",
            render: (e, table) => {
                if (table.schema == undefined) {
                    return "";
                }

                let cols = table.schema.columns.map(c => {return (
                    <>
                        <Col span={3}>{c.name}</Col>
                        <Col span={15}>{c.type}</Col>
                        <Col span={6}>{c.comment}</Col>
                    </>
                )});

                let pts = table.schema.partitions.map(c => {return (
                    <>
                        <Col span={3}>{c.name}</Col>
                        <Col span={15}>{c.type}</Col>
                        <Col span={6}>{c.comment}</Col>
                    </>
                )});

                let ptTip = pts.length > 0 ? <Col span={24}><br /><FMSpan id="pages.Source.SourceDb.components.TableList.partition" defaultMessage="分区列" /></Col>: "";
                let constraint: any = [];
                if (table.schema.tableConstraints) {
                    constraint = [
                        <Col span={24}><br />table constraint</Col>
                    ]

                    const tcList = table.schema.tableConstraints;

                    for (const tc of tcList) {
                        if (tc.primaryKeys) {
                            constraint.push(<Col span={3}>primary keys: </Col>);
                            constraint.push(<Col span={21}>{tc.primaryKeys?.join(", ")} </Col>);
                        }

                        if (tc.foreignKeyConstraint) {
                            constraint.push(<Col span={5}>foreign key constraint</Col>);
                            constraint.push(<Col span={19}>{JSON.stringify(tc.foreignKeyConstraint)}</Col>);
                        }
                    }
                }

                return (
                    <>
                        <Row>
                            <Col span={24}><FMSpan id="pages.Source.SourceDb.components.TableList.column" defaultMessage="列名/类型" /></Col>
                            {cols}
                            {ptTip}
                            {pts}
                            {constraint}
                        </Row>
                    </>
                )
            },
            renderText: (text, record, index, action) => {
                return JSON.stringify(record.schema, null, 4);
            },
            hideInTable: true,
            search: false
        },
        {
           title: fm(intl, "pages.Source.SourceDb.components.TableList.lifecycle", "生命周期"),
           dataIndex: "lifecycle",
           hideInTable: true,
           hideInSearch: true
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.TableList.operation", "操作"),
            dataIndex: 'option',
            valueType: 'option',
            render: (row, entity) => {
                const options = [
                    <a key={"1"} onClick={() => {
                        setShowDetail(true);
                        setCurrentRow(entity);
                    }}>
                        <FMSpan id="pages.Source.SourceDb.components.TableList.detail" defaultMessage="详情" />
                    </a>,
                ];

                return options;
            },
            search: false,
        }
    ];

    // {hasPartitions: 'ascend'}
    // {partitions: 'ascend'}
    // {partitions: 'descend'}

    return (
        <>
            <ProTable<API.TableModel>
                columns={columns}
                request={async (params, sort, filter) => {
                    params["dbId"] = props.db.id;
                    let res = await getTables(params, sort, filter);
                    setTotal(res?.total || 0);
                    return res;
                }}
                rowKey="id"
                pagination={{
                    showQuickJumper: true,
                    size: 'default',
                    responsive: true,
                    totalBoundaryShowSizeChanger: 10,
                    total: total
                }}
                search={{
                    filterType: "light"
                }}
                rowSelection={{
                    // 注释该行则默认不显示下拉选项
                    selections: [Table.SELECTION_ALL, Table.SELECTION_INVERT],
                    onChange: (selectedRowKeys: Key[], selectedRows: API.TableModel[], info: { type: RowSelectMethod }) => {
                        setTables(selectedRows.map((t) => t.name))
                    }
                }}
                toolbar={{
                    actions: [
                        <Button
                            key="key"
                            type="primary"
                            onClick={() => {
                                setNewJobFormOpen(true);
                            }}
                        >
                            <FMSpan id="pages.Source.SourceDb.components.TableList.newJob" defaultMessage="新建迁移任务" />
                        </Button>,
                    ]
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
                <ProDescriptions<API.TableModel>
                    column={2}
                    title={currentRow?.name}
                    bordered={true}
                    request={async () => ({
                        data: currentRow || {},
                    })}
                    params={{
                        id: currentRow?.id,
                    }}
                    columns={columns as ProDescriptionsItemProps<API.TableModel>[]}
                />
            </Drawer>

            <NewJobForm sourceName={props.db.sourceName} dbName={props.db.name} jobType="tables" tables={tables} open={newJobFormOpen} setOpen={setNewJobFormOpen} />
        </>
    )
};