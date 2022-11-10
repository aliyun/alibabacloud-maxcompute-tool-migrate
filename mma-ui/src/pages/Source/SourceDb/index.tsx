import {PageContainer} from "@ant-design/pro-components";
import TableList from "./components/TableList";
import {useParams} from "@@/exports";
import {useEffect, useState} from "react";
import {Button, Card, Descriptions} from "antd";
import {getDbs} from "@/services/source";
import PartitionList from "@/pages/Source/SourceDb/components/PartitionList";

export default (props: any) => {
    const [tabKey, setTabKey] = useState("tables");
    const [database, setDatabase] = useState<API.DbModel>();

    const params = new URLSearchParams(window.location.search)

    let dbId = parseInt(params.get("dbId") || "");

    const getDb = () => {
        getDbs({id: dbId}).then((res) => {
            let dbs = res.data;
            //@ts-ignore
            let db = dbs[0];
            setDatabase(db);
        })
    }

    useEffect(() => {
        getDb();
    }, [dbId]);

    const DbDetail = (props: {db?: API.DbModel, tabKey: string}) => {
        let db = props?.db;
        if (db === undefined) {
            return <div>加载不到数据</div>
        }

        if (props.tabKey == "tables") {
            return <TableList db={db} />
        } else {
            return <PartitionList db={db} />
        }
    };

    return (
        <PageContainer
            header={{
                title: database?.name,
            }}
            tabList={[
                {
                    tab: 'table列表',
                    key: 'tables',
                },
                {
                    tab: 'partition列表',
                    key: 'partitions',
                },
            ]}
            onTabChange={ (activeKey: string) => {
                setTabKey(activeKey);
            }}
            content={(
                <Descriptions column={3}>
                    <Descriptions.Item label="数据源">{database?.sourceName}</Descriptions.Item>
                    <Descriptions.Item label="表总数">{database?.tables}</Descriptions.Item>
                    <Descriptions.Item label="迁移完成的表">{database?.tablesDone}</Descriptions.Item>
                    <Descriptions.Item label="迁移中的表">{database?.tablesDoing}</Descriptions.Item>
                    <Descriptions.Item label="迁移失败的表">{database?.tablesFailed}</Descriptions.Item>
                    <Descriptions.Item label="分区总数">{database?.partitions}</Descriptions.Item>
                    <Descriptions.Item label="迁移完成的分区">{database?.partitionsDone}</Descriptions.Item>
                    <Descriptions.Item label="迁移中的分区">{database?.partitionsDoing}</Descriptions.Item>
                    <Descriptions.Item label="迁移失败的分区">{database?.partitionsFailed}</Descriptions.Item>
                </Descriptions>
            )}
            extra={[
                <Button key="updatePage" onClick={getDb}>刷新</Button>,
                // <Button key="updateDbMeta" type="primary">更新元数据</Button>
            ]}
        >
            <DbDetail tabKey={tabKey} db={database} />
        </PageContainer>
    )
}