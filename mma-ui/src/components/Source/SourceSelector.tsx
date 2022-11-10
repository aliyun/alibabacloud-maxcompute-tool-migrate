import {useEffect, useState} from "react";
import {getSources} from "@/services/source";
import {ProFormSelect} from "@ant-design/pro-components";

export const SourceSelectorById = () => {
    const [sourceEnum, setSourceEnum] = useState<Record<string, {text: string}>>({})

    useEffect(() => {
        getSources().then((res) => {
            let se: Record<string, {text: string}> = {};
            for (let s of res?.data || []) {
                se[s.id] = {text: s.name};
            }

            setSourceEnum(se);
        })
    }, []);

    return (
        <ProFormSelect
            showSearch
            name="sourceId"
            valueEnum={sourceEnum}
        />
    );
}


export const SourceSelectorByName = () => {
    const [sourceEnum, setSourceEnum] = useState<Record<string, {text: string}>>({})

    useEffect(() => {
        getSources().then((res) => {
            let se: Record<string, {text: string}> = {};
            for (let s of res?.data || []) {
                se[s.name] = {text: s.name};
            }

            setSourceEnum(se);
        })
    }, []);

    return (
        <ProFormSelect
            showSearch
            name="sourceName"
            valueEnum={sourceEnum}
        />
    );
}