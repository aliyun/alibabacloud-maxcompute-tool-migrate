import {ProColumns, ProTable, ProDescriptions} from "@ant-design/pro-components";
import {formatSize, showErrorMsg} from "@/utils/format";
import {getDbs, getPts, getTables, resetPts} from "@/services/source";
import React, {useState} from "react";
import {Button, message, Popconfirm, Table} from "antd";
import {Key, RowSelectMethod} from "antd/lib/table/interface";
import {NewPartitionJobForm} from "@/components/Job/NewPartitionJobForm";
import {useIntl} from "umi";
import {FMSpan, fm} from "@/components/i18n";

export default (props: {db: API.DbModel}) => {
    const [total, setTotal] = useState<number>(0);
    const [pts, setPts] = useState<API.PartitionModel[]>([]);
    const [visible, setVisible] = useState<boolean>(false);
    const intl = useIntl();

    const columns: ProColumns<API.PartitionModel>[] = [
        {
            title: "Schema",
            dataIndex: "schemaName",
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.name", "表名"),
            dataIndex: "tableName",
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.value", "分区值"),
            dataIndex: "value",
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.size", "大小"),
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
                multiple: 2
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.row", "行数"),
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
                multiple: 1
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.updated", "元数据有更新"),
            dataIndex: "updated",
            render: (_, entity) => {
                return entity.updated ? "yes": "no";
            },
            valueType: "select",
            valueEnum: {
                0: {text: 'yes'},
                1: {text: 'no'},
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.lastDdlTime", "数据最后修改时间"),
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
            title: fm(intl, "pages.Source.SourceDb.components.PartitionList.status", "状态"),
            dataIndex: 'status',
            valueType: 'checkbox',
            valueEnum: {
                INIT: {text: fm(intl, "pages.Source.SourceDb.components.PartitionList.statsInit", "未迁移")},
                DONE: {text: fm(intl, "pages.Source.SourceDb.components.PartitionList.statusDone", "完成")},
                DOING: {text: fm(intl, "pages.Source.SourceDb.components.PartitionList.statusDoing", "迁移中")},
                FAILED: {text: fm(intl, "pages.Source.SourceDb.components.PartitionList.statusFailed", "失败")},
            },
            filters: false,
        },
    ];

    return (
        <>
            <ProTable<API.PartitionModel>
                columns={columns}
                request={async (params, sort, filter) => {
                    params["dbId"] = props?.db.id;
                    let res = await getPts(params, sort, filter);
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
                    onChange: (selectedRowKeys: Key[], selectedRows: API.PartitionModel[], info: { type: RowSelectMethod }) => {
                        setPts(selectedRows);
                    }
                }}
                toolbar={{
                    actions: [
                        // <Popconfirm
                        //     key={"1"}
                        //     title="确定重置分区状态?"
                        //     onConfirm={async () => {
                        //         try {
                        //             let hide = message.loading("重置分区状态中..")
                        //             let res = await resetPts(ptIds);
                        //             hide();
                        //             if (res.success) {
                        //                 message.success(`success to update ${res.data} partitions`, 10);
                        //             } else {
                        //                 showErrorMsg(res);
                        //             }
                        //         } catch (e) {
                        //             message.error(`unexpected error ${e}`);
                        //         }
                        //     }}
                        //     okText="Yes"
                        //     cancelText="No"
                        // >
                        //     <Button type="primary">
                        //         重置状态
                        //     </Button>
                        // </Popconfirm>,

                        <Button type="primary"
                                key="2"
                                onClick={() => {
                                    setVisible(true);
                                }}
                        >
                            <FMSpan id="pages.Source.SourceDb.components.PartitionList.newMigration" defaultMessage="新建迁移任务"/>
                        </Button>

                    ]
                }}
            />

            <NewPartitionJobForm db={props.db}  visible={visible} setVisible={setVisible} partitions={pts} />
        </>
    )
};