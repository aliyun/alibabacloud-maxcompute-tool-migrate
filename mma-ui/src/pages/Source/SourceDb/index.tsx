import {PageContainer} from "@ant-design/pro-components";
import TableList from "./components/TableList";
import {useEffect, useState} from "react";
import {Button, Card, Descriptions} from "antd";
import {getDbs, getDb, getSource} from "@/services/source";
import PartitionList from "@/pages/Source/SourceDb/components/PartitionList";
import {useIntl} from "umi";
import {FMSpan, FM, fm} from "@/components/i18n";
import {ReloadOutlined} from "@ant-design/icons";

export default (props: any) => {
    const [tabKey, setTabKey] = useState("tables");
    const [database, setDatabase] = useState<API.DbModel>();
    const params = new URLSearchParams(window.location.search)
    const intl = useIntl();

    let sourceId = parseInt(params.get("sourceId") || "");
    let dbId = parseInt(params.get("dbId") || "");

    const initDb = () => {
        getDb(sourceId, dbId).then((res) => {
            let db = res;

            getSource(sourceId).then((res1) => {
                console.log(res1);
                db.sourceName = res1?.data?.name ?? "";

                setDatabase(db);
            });
        })
    }

    useEffect(() => {
        initDb();
    }, [sourceId, dbId]);

    const DbDetail = (props: {db?: API.DbModel, tabKey: string}) => {
        let db = props?.db;
        if (db === undefined) {
            return <div><FMSpan id="pages.Source.SourceDb.failedToGetData"  defaultMessage="加载数据失败" /></div>
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
                    tab: fm(intl, "pages.Source.SourceDb.TableList", "Table列表"),
                    key: 'tables',
                },
                {
                    tab: fm(intl, "pages.Source.SourceDb.PartitionList", "Partition列表"),
                    key: 'partitions',
                },
            ]}
            onTabChange={ (activeKey: string) => {
                setTabKey(activeKey);
            }}
            content={(
                <Descriptions column={3}>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.datasource", "数据源")}>{database?.sourceName}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.tables", "表总数")}>{database?.tables}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.tablesOk", "迁移完成的表")}>{database?.tablesDone}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.tablesDoing", "迁移中的表")}>{database?.tablesDoing}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.tablesFailed", "迁移失败的表")}>{database?.tablesFailed}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.partitions", "分区总数")}>{database?.partitions}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.partitionsDone", "迁移完成的分区")}>{database?.partitionsDone}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.PartitionsDoing", "迁移中的分区")}>{database?.partitionsDoing}</Descriptions.Item>
                    <Descriptions.Item label={fm(intl, "pages.Source.SourceDb.PartitionFailed", "迁移失败的分区")}>{database?.partitionsFailed}</Descriptions.Item>
                </Descriptions>
            )}
            extra={[
                <Button type="text" key="updatePage" onClick={initDb}>
                    <ReloadOutlined />
                </Button>,
                // <Button key="updateDbMeta" type="primary">更新元数据</Button>
            ]}
        >
            <DbDetail tabKey={tabKey} db={database} />
        </PageContainer>
    )
}
