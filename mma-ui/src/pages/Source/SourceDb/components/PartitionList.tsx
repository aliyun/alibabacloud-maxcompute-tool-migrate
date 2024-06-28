import {ProColumns, ProTable, ProDescriptions} from "@ant-design/pro-components";
import {formatSize, showErrorMsg} from "@/utils/format";
import {getDbs, getPts, getTables, resetPts} from "@/services/source";
import React, {useState} from "react";
import {Button, message, Popconfirm, Table} from "antd";
import {Key, RowSelectMethod} from "antd/lib/table/interface";
import {NewPartitionJobForm} from "@/components/Job/NewPartitionJobForm";

export default (props: {db: API.DbModel}) => {
    const [total, setTotal] = useState<number>(0);
    const [pts, setPts] = useState<API.PartitionModel[]>([]);
    const [visible, setVisible] = useState<boolean>(false);

    const columns: ProColumns<API.PartitionModel>[] = [
        {
            title: "schema",
            dataIndex: "schemaName",
        },
        {
            title: "表名",
            dataIndex: "tableName",
        },
        {
            title: "分区值",
            dataIndex: "value",
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
                multiple: 2
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
                multiple: 1
            }
        },
        {
            title: "元数据有更新",
            dataIndex: "updated",
            render: (_, entity) => {
                return entity.updated ? "是": "否";
            },
            valueType: "select",
            valueEnum: {
                0: {text: '否'},
                1: {text: '是'},
            }
        },
        {
            title: "数据最后修改时间",
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
            title: "状态",
            dataIndex: 'status',
            valueType: 'checkbox',
            valueEnum: {
                INIT: {text: '未迁移'},
                DONE: {text: '完成'},
                DOING: {text: '迁移中'},
                FAILED: {text: '失败'},
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
                            新建迁移任务
                        </Button>

                    ]
                }}
            />

            <NewPartitionJobForm db={props.db}  visible={visible} setVisible={setVisible} partitions={pts} />
        </>
    )
};