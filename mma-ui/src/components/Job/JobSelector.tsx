import {useEffect, useRef, useState} from "react";
import {getSources} from "@/services/source";
import {ProFormSelect} from "@ant-design/pro-components";
import {getJobBasicInfo} from "@/services/job";
import {InputRef, } from "antd";

export const JobSelector = (props: {initialValue: string}) => {
    const [idToName, setIdToName] = useState<API.IdToName>({})

    useEffect(() => {
        getJobBasicInfo().then((res) => {
            setIdToName(res?.data || []);
        })
    }, []);

    return (
        <ProFormSelect
            showSearch
            name="jobId"
            valueEnum={idToName}
            initialValue={props.initialValue}
        />
    );
}