import type {ProColumns} from '@ant-design/pro-components';
import {ActionType, PageContainer, ProTable, TableDropdown} from '@ant-design/pro-components';
import {Button, Drawer, message, Tabs, Modal, Dropdown} from 'antd';
import {getSources, runSourceInitializer, updateSource} from "@/services/source";
import {Link, useIntl} from "umi";
import React, {useRef, useState} from "react";
import {SourceLoader} from "@/pages/Source/SourceList/components/SourceLoader";
import {SOURCES_ROUTE, SOURCES_NEW} from "@/constant";
import {ActionLog} from "@/pages/Source/SourceList/components/ActionLog";
import {FMSpan, FM, fm} from "@/components/i18n";

export default () => {
    const aRef = useRef<ActionType>();
    const [progressVisible, setProgressVisible] = useState(false);
    const [currentRow, setCurrentRow] = useState<API.DataSource>();
    const [loading, setLoading] = useState<Record<number, boolean>>({});
    const [showDetail, setShowDetail] = useState<boolean>(false);
    const [showConfirmInit, setShowConfirmInit] = useState<boolean>(false);
    const [showConfirmUpdate, setShowConfirmUpdate] = useState<boolean>(false);
    const [sourceDetailKey, setSourceDetailKey] = useState<string>("init_log");
    const [loaderId, setLoaderId] = useState<number>(0);

    const intl = useIntl();

    const columns: ProColumns<API.DataSource>[] = [
        {
            title: FM("pages.Source.SourceList.sourceName", "名称"),
            dataIndex: 'name',
            render: (_, entity ) => <Link to={SOURCES_ROUTE + "/" + entity.name}>{_}</Link>,
            formItemProps: {
                lightProps: {
                    labelFormatter: (value) => `app-${value}`,
                },
            },
        },
        {
            title: FM("pages.Source.SourceList.type", "类型"),
            dataIndex: 'type',
        },
        {
            title: FM("pages.Source.SourceList.dbNum", "db数"),
            dataIndex: 'dbNum',
        },
        {
            title: FM("pages.Source.SourceList.tableNum", "table数"),
            dataIndex: 'tableNum',
        },
        {
            title: FM("pages.Source.SourceList.partitionNum", "partition数"),
            dataIndex: 'partitionNum',
        },
        {
            title: FM("pages.Source.SourceList.initStatus", "初始化"),
            dataIndex: 'initStatus',
            render: (row: any, entity: API.DataSource) => {
                if (entity.initStatus == "NOT_YET") {
                    return "__"
                }

                return (
                    <a
                        onClick={() => {
                            setSourceDetailKey("init_log");
                            setShowDetail(true);
                            setCurrentRow(entity);
                        }
                    }>
                        {entity.initStatus}
                    </a>
                )
            }
        },
        {
            title: FM("pages.Source.SourceList.lastUpdateTime", "最新更新"),
            dataIndex: 'lastUpdateTime',
            valueType: 'dateTime',
            render: (row: any, entity: API.DataSource) => {
                return (
                    <a
                        onClick={() => {
                            setSourceDetailKey("update_log");
                            setShowDetail(true);
                            setCurrentRow(entity);
                        }
                        }>
                        {entity.lastUpdateTime}
                    </a>
                )
            }
        },
        {
            title: FM("pages.Source.SourceList.operation", "操作"),
            width: '164px',
            key: 'option',
            valueType: 'option',
            render: (row, entity) => {
                const buttons = [
                    <Button
                        key="update"
                        type="link"
                        onClick={() => {
                            setCurrentRow(entity);
                            setShowConfirmUpdate(true);
                        }}
                        style={{padding: "0px"}}
                    >
                        <FMSpan id="pages.Source.SourceList.update" defaultMessage="更新" />
                    </Button>,
                ];

                if (entity.type == "DATABRICKS" || entity.type == "HIVE_GLUE" || entity.type == "HIVE") {
                    buttons.push(
                        <Button
                            key="init"
                            type="link"
                            disabled={loading[entity.id] && entity.initStatus == "RUNNING"}
                            loading={loading[entity.id]}
                            onClick={async (event) => {
                                setCurrentRow(entity);
                                setShowConfirmInit(true);
                            }}
                            style={{padding: "0px"}}
                        >
                            <FMSpan id="pages.Source.SourceList.init" defaultMessage="初始化" />
                        </Button>
                    );
                }

                return  buttons;
            },
        },
    ];

    return (
        <PageContainer>
            <ProTable<API.DataSource>
                columns={columns}
                actionRef={aRef}
                request={(params, sorter, filter) => {
                    // 表单搜索项会从 params 传入，传递给后端接口。
                    return getSources();
                }}
                rowKey="id"
                search={false}
                toolbar={{
                    actions: [
                        <Link
                            key="key"
                            type="primary"
                            to={SOURCES_NEW}
                        >
                            <Button type="primary">
                                <FMSpan id="pages.Source.SourceList.addSource" defaultMessage="添加数据源" />
                            </Button>
                        </Link>,
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
                {currentRow?.id && (
                    <>
                        <Tabs defaultActiveKey={sourceDetailKey}>
                            <Tabs.TabPane tab={fm(intl, "pages.Source.SourceList.initLog", "初始化日志")} key="init_log">
                                <ActionLog sourceId={currentRow.id} actionType={"DATASOURCE_INIT"} />
                            </Tabs.TabPane>
                            <Tabs.TabPane tab={fm(intl, "pages.Source.SourceList.updateLog", "元数据更新日志")} key="update_log">
                                <ActionLog sourceId={currentRow.id} actionType={"DATASOURCE_UPDATE"} />
                            </Tabs.TabPane>
                        </Tabs>
                    </>
                )}
            </Drawer>
            <Modal
                title="确认运行"
                open={showConfirmInit}
                onOk={async () => {
                    setShowConfirmInit(false);

                    if (loading[currentRow.id]) {
                        return;
                    }

                    let newLoading = {...loading};
                    newLoading[currentRow.id] = true;
                    setLoading(newLoading);

                    const res = await runSourceInitializer(currentRow.id);
                    if (! res.success) {
                        message.error(res.message, 30);
                    }

                    newLoading = {...loading};
                    newLoading[currentRow.id] = false;
                    setLoading(newLoading);
                    aRef.current?.reload();
                }}
                onCancel={() => {
                    setShowConfirmInit(false);
                }}
            >
                {(
                    currentRow?.initStatus == "OK" &&
                    fm(intl, "pages.Source.SourceList.warnTip", "该数据源已经已经运行过初始化, 请确认是否重新运行初始化")
                ) || ""}
            </Modal>

            <Modal
                title={fm(intl, "pages.Source.SourceList.modalTitle", "确认")}
                open={showConfirmUpdate}
                onOk={() => {
                    setShowConfirmUpdate(false);

                    if (loading[currentRow.id]) {
                        return
                    }

                    let newLoading = {...loading};
                    newLoading[currentRow.id] = true;
                    setLoading(newLoading);

                    setProgressVisible(true);
                    setLoaderId(loaderId + 1);

                    newLoading = {...loading};
                    newLoading[currentRow.id] = false;
                    setLoading(newLoading);
                }}
                onCancel={() => setShowConfirmUpdate(false)}
            >
                <FMSpan id="pages.Source.SourceList.sureToUpdateMeta" defaultMessage="是否更新元数据" />
            </Modal>
            <SourceLoader dataSource={currentRow} visible={progressVisible} setVisible={setProgressVisible} loaderId={loaderId} />
        </PageContainer>
    );
};

