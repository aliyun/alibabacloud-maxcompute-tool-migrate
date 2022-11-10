import {useEffect, useState} from "react";
import {getDstMcProjects} from "@/services/config";
import {ProFormSelect} from "@ant-design/pro-components";

export const McProjectSelector = (props: {keyName: string}) => {
    const [projects, setProjects] = useState<Record<string, string>>({});

    useEffect(() => {
        getDstMcProjects().then(res => {
            let m: Record<string, string> = {};

            for (let s of res?.data || []) {
                m[s] = s;
            }

            setProjects(m);
        })
    }, []);

    return (
        <ProFormSelect
            showSearch
            name={props.keyName}
            valueEnum={projects}
        />
    )
}