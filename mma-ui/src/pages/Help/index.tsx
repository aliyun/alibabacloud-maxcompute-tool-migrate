import {PageContainer} from "@ant-design/pro-components";
import {useEffect, useState} from "react";
import {getMMAVersion} from "@/services/config";

export default () => {
    const [mmaVersion, setMmaVersion] = useState<String>("MMAv3");

    useEffect(() => {
        getMMAVersion().then((res) => {
            setMmaVersion(`MMAv3-${res}`);
        })
    }, []);

    return (
        <PageContainer>
            <ul>版本: {mmaVersion}</ul>
            <ul>
                <li><a href="/MMAv3-manual.pdf">使用文档</a></li>
                <li><a href="/mma-udtf-hive1.jar">Hive UDTF jar for hive1下载</a></li>
                <li><a href="/mma-udtf-hive2.jar">Hive UDTF jar for hive2下载</a></li>
                <li><a href="/mma-udtf-hive3.jar">Hive UDTF jar for hive3下载</a></li>
            </ul>
        </PageContainer>
    )
}