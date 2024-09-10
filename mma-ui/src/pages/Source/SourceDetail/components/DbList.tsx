import {ProColumns, ProTable, ProDescriptions} from "@ant-design/pro-components";
import {Card, Descriptions, Drawer, Switch} from "antd";
import {getDbs, getPts} from "@/services/source";
import {formatSize} from "@/utils/format";
import {Link, useIntl} from "umi";
import React, {useState} from "react";
import {NewJobForm} from "@/components/Job/NewJobForm";
import {SOURCES_ROUTE} from "@/constant";
import {FMSpan, fm, FM} from "@/components/i18n";

export default (props?: { source?: API.DataSource }) => {
    const ds = props?.source;
    const [currentDb, setCurrentDb] = useState<API.DbModel>();
    const [newJobFormOpen, setNewJobFormOpen] = useState<boolean>(false);
    const [total, setTotal] = useState<number>(0);
    const intl = useIntl();

    const columns: ProColumns<API.DbModel>[] = [
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.name", "库名"),
            dataIndex: 'name',
            render: (dom, entity) => {
                return (<Link to={`${SOURCES_ROUTE}/${ds?.name}/${entity.name}?sourceId=${ds?.id}&dbId=${entity.id}`}>{entity.name}</Link>);
            }
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.tables", "table总数"),
            search: false,
            dataIndex: "tables",
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.tablesDone", "迁移完成table数"),
            search: false,
            dataIndex: "tablesDone",
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.tablesFailed", "有错误的table数"),
            search: false,
            dataIndex: "tablesFailed",
            hideInTable: true
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.tablesPartDone", "部分完成table数"),
            search: false,
            dataIndex: "tablesPartDone",
            hideInTable: true
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.tablesDoing", "正在迁移table数"),
            search: false,
            dataIndex: "tablesDoing",
            hideInTable: true
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.partitions", "分区总数"),
            search: false,
            dataIndex: 'partitions',
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.partitionsDone", "迁移完成分区数"),
            search: false,
            dataIndex: 'partitionsDone',
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.partitionsFailed", "出错的分区数"),
            search: false,
            dataIndex: "partitionsFailed",
            hideInTable: true
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.partitionsDoing", "正在迁移分区数"),
            search: false,
            dataIndex: "partitionsDoing",
            hideInTable: true
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.size", "大小"),
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
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.rows", "行数"),
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
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.status", "状态"),
            dataIndex: 'status',
            valueType: 'select',
            valueEnum: {
                INIT: {text: fm(intl, "pages.Source.SourceDetail.components.DbList.statsInit", "未迁移")},
                DONE: {text: fm(intl, "pages.Source.SourceDetail.components.DbList.statsDone", "完成")},
                DOING: {text: fm(intl, "pages.Source.SourceDetail.components.DbList.statusDoing", "迁移中")},
                FAILED: {text: fm(intl, "pages.Source.SourceDetail.components.DbList.statusFailed", "部分完成，有失败")},
                PART_DONE: {text: fm(intl, "pages.Source.SourceDetail.components.DbList.statusPartDone", "部分完成")},
            },
            filters: true,
        },
        {
            title: fm(intl, "pages.Source.SourceDetail.components.DbList.operation", "操作"),
            render: (row, entity) => {
                return [
                    <a key={1} onClick={() => {
                        setNewJobFormOpen(true);
                        setCurrentDb(entity);
                    }}>
                        <FMSpan id="pages.Source.SourceDetail.components.DbList.migration" defaultMessage="迁移" />
                    </a>
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
                                <Descriptions.Item key={1} label={fm(intl, "pages.Source.SourceDetail.components.DbList.dbs", "数据库")}>{ds.dbNum}</Descriptions.Item>
                                <Descriptions.Item key={2}  label={fm(intl, "pages.Source.SourceDetail.components.DbList.tables", "表")}>{ds.tableNum}</Descriptions.Item>
                                <Descriptions.Item key={3}  label={fm(intl, "pages.Source.SourceDetail.components.DbList.partitions", "分区")}>{ds.partitionNum}</Descriptions.Item>
                                <Descriptions.Item key={4}  label={fm(intl, "pages.Source.SourceDetail.components.DbList.lastUpdateTime", "最新更新时间")}>{ds.lastUpdateTime}</Descriptions.Item>
                            </Descriptions>
                        </Card>
                    );
                }}
            />

            <NewJobForm sourceName={currentDb?.sourceName ?? ""} dbName={currentDb?.name ?? ""} jobType="database" open={newJobFormOpen} setOpen={setNewJobFormOpen} />
        </>
    );
}
