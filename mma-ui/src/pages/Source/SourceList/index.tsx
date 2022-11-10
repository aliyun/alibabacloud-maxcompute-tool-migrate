import {QuestionCircleOutlined} from '@ant-design/icons';
import type {ProColumns} from '@ant-design/pro-components';
import {ActionType, PageContainer, ProTable, TableDropdown} from '@ant-design/pro-components';
import {Button, message, Tooltip} from 'antd';
import {getLoadingMetaProgress, getSources, updateSource} from "@/services/source";
import {Link} from "umi";
import React, {useRef, useState} from "react";
import {SourceLoader} from "@/pages/Source/SourceList/components/SourceLoader";
import {SOURCES_ROUTE, SOURCES_NEW} from "@/constant";

export default () => {
    const aRef = useRef<ActionType>();
    const [visible, setVisible] = useState(false);
    const [currentRow, setCurrentRow] = useState<API.DataSource>();

    const columns: ProColumns<API.DataSource>[] = [
        {
            title: '数据源名',
            dataIndex: 'name',
            render: (_, entity ) => <Link to={SOURCES_ROUTE + "/" + entity.name}>{_}</Link>,
            formItemProps: {
                lightProps: {
                    labelFormatter: (value) => `app-${value}`,
                },
            },
        },
        {
            title: '类型',
            dataIndex: 'type',
        },
        {
            title: 'db数',
            dataIndex: 'dbNum',
        },
        {
            title: 'table数',
            dataIndex: 'tableNum',
        },
        {
            title: 'partition数',
            dataIndex: 'partitionNum',
        },
        {
            title: "最新更新",
            dataIndex: 'lastUpdateTime',
            valueType: 'dateTime',
        },
        {
            title: '操作',
            width: '164px',
            key: 'option',
            valueType: 'option',
            render: (row, entity) => [
                <a key="update"
                   onClick={() => {
                       setCurrentRow(entity);
                       setVisible(true);
                   }}
                >更新</a>,
            ],
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
                            <Button type="primary">添加数据源</Button>
                        </Link>,
                    ]
                }}
            />

            <SourceLoader dataSource={currentRow} visible={visible} onCancel={() => {setVisible(false)}} />
        </PageContainer>
    );
};

