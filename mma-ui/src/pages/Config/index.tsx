import services, {pingMMA, restartMMA, updateMMAConfig} from "@/services/config";
import React from "react";
import {PageContainer} from '@ant-design/pro-components';
import ConfigTable from "@/components/Config/ConfigTable";
import {message} from "antd";
import {useModel} from "@@/exports";
const { getMMAConfig, saveMMAConfig } = services;

const MMAConfigTable: React.FC = () => {
    let taskMaxNum = "0";

    const { initialState, loading, error, refresh, setInitialState } = useModel('@@initialState');

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
            let hide = message.loading("修了task.max.num, 服务正在重启");
            taskMaxNum = newTaskMaxNum.toString();
            await restartMMA();

            while (true) {
                try {
                    await pingMMA();
                    hide();
                    message.success("重启完毕");
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