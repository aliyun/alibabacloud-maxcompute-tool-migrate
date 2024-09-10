import services, {pingMMA, restartMMA, updateMMAConfig} from "@/services/config";
import React from "react";
import {PageContainer} from '@ant-design/pro-components';
import ConfigTable from "@/components/Config/ConfigTable";
import {message} from "antd";
import {useModel} from "@@/exports";
import {useIntl} from "umi";
import {fm} from "@/components/i18n";

const { getMMAConfig, saveMMAConfig } = services;

const MMAConfigTable: React.FC = () => {
    let taskMaxNum = "0";

    const { initialState, loading, error, refresh, setInitialState } = useModel('@@initialState');
    const intl = useIntl();

    const saveMMAFunc = initialState?.inited ?? false ? updateMMAConfig : saveMMAConfig;

    return (
        <PageContainer
            content={ <ConfigTable request={getConfig} onSave={saveConfig} /> }
            ghost={true}
        >

        </PageContainer>
    )

    async function getConfig() {
        let res = await getMMAConfig();
        let configs = res.data || [];
        for (let config of configs) {
            if (config.key === "task.max.num") {
                taskMaxNum = config.value.toString();
                break;
            }
        }

        return res;
    }

    async function saveConfig(c: API.MMAConfigJson) {
        let newTaskMaxNum = c["task.max.num"];
        let res = await saveMMAFunc(c);
        setInitialState({inited: true});

        if (newTaskMaxNum != taskMaxNum) {
            let hide = message.loading(fm(intl, "pages.config.serviceRestarting", "服务正在重启"));
            taskMaxNum = newTaskMaxNum.toString();
            await restartMMA();

            while (true) {
                try {
                    await pingMMA();
                    hide();
                    message.success(fm(intl,"pages.config.serviceRestarted", "重启完毕"));
                    break;
                } catch (e) {

                }

                await sleep(1000);
            }
        }

        return res;
    }
}

function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}


export default MMAConfigTable;