import { PageContainer } from '@ant-design/pro-components';
import {checkConfigStatus} from "@/services/config";
import {history} from "umi";
import {Spin} from "antd";
import {SOURCES_ROUTE} from "@/constant";
import {createContext, useEffect, useState} from "react";
import {set} from "husky";

const useMMAInit = () => {
    const [init, setInit] = useState<boolean>(false);

    useEffect(() => {
        checkConfigStatus().then((res) => {
            if (!res.data?.inited) {
                setInit(false);
            } else {
                setInit(true);
            }
        });
    }, []);

    return init;
}

const HomePage: React.FC = () => {
    // const init = useMMAInit();
    //
    // if (init) {
    //     history.push("/config");
    // } else {
    //     history.push(SOURCES_ROUTE);
    // }

    // checkConfigStatus().then((res) => {
    //     if (!res.data?.inited) {
    //         history.push("/config");
    //     } else {
    //         history.push(SOURCES_ROUTE);
    //     }
    // });
    return (
        <PageContainer ghost>
            <Spin tip="Loading...">
            </Spin>
        </PageContainer>
    );
};

export default HomePage;
