import {PageContainer, ProBreadcrumb} from "@ant-design/pro-components";
import {useParams } from "@umijs/max";
import * as React from "react";
import {useEffect, useState} from "react";
import {getSource, getSourceByName, getSourceConfig, updateSourceConfig} from "@/services/source";
import DbList from "./components/DbList";
import ConfigTable from "@/components/Config/ConfigTable";
import {SourceLoadingProgress} from "@/components/Source/SourceLoadingProgress";
import {FMSpan, fm} from "@/components/i18n";
import {useIntl} from "umi";

const SourceDetail = (props: { source?: API.DataSource, tabKey?: string }) => {
    let source = props.source;
    let [currentSource, setCurrentSource] = useState<API.DataSource>();

    const [visible, setVisible] = useState(false);

    let Detail = () => {
        if (source === undefined) {
            return(
                <div><FMSpan id="pages.Source.SourceDetail.refresh" defaultMessage="请刷新页面" /></div>
            )
        }

        if (props?.tabKey === "data") {
            return <DbList source={source} />
        } else {
            return <ConfigTable
                request={() => getSourceConfig(source?.name as string)}
                onSave={(c) => updateSourceConfig(source?.id as number, c) }
                afterSave={() => {
                    //message.success("success");
                    // setCurrentSource(source);
                    // setVisible(true);
                }}
            />;
        }
    }

    return (
        <>
            <Detail />
            <SourceLoadingProgress
                dataSource={currentSource} visible={visible}
                onCancel={() => {setVisible(false); setCurrentSource(undefined)}}
            />
        </>

    )
}

export default () => {
    const params = useParams();
    const [source, setSource] = useState<API.DataSource>();
    const [tabKey, setTabKey] = useState("data");
    const intl = useIntl();

    useEffect(() => {
        getSourceByName(params?.name || "0", true)
            .then((res) => setSource(res.data));
    }, [params]);

    return (
        <PageContainer
            header={{
                title: source?.name,
            }}
            tabList={[
                {
                    tab: fm(intl, "pages.Source.SourceDetail.data", '数据信息'),
                    key: 'data',
                },
                {
                    tab: fm(intl, "pages.Source.SourceDetail.config", '配置信息'),
                    key: 'config',
                },
            ]}

            onTabChange={ (activeKey: string) => {
                setTabKey(activeKey);
            }}
        >
            <SourceDetail source={source} tabKey={tabKey}/>
        </PageContainer>
    )
}