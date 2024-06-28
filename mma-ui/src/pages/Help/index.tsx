import {
    PageContainer,
    ProForm,
    ProFormDependency, ProFormDigit,
    ProFormField, ProFormGroup, ProFormInstance, ProFormList,
    ProFormRadio, ProFormSwitch,
    ProFormText, ProFormTextArea
} from "@ant-design/pro-components";
import React, {useEffect, useRef, useState} from "react";
import {getMMAVersion} from "@/services/config";
import {ProFormTimerPicker} from "@/components/TimerPicker";
import {Form, Button, Input, message, InputRef} from "antd";
import { PlusOutlined } from '@ant-design/icons';
import {
    ModalForm,
    ProFormDateRangePicker,
    ProFormSelect,
} from '@ant-design/pro-components';
import {getJobOptions, submitJob} from "@/services/job";
import {showErrorMsg} from "@/utils/format";

export default () => {
    const [mmaVersion, setMmaVersion] = useState<String>("MMAv3");
    const [form] = Form.useForm<{ name: string; company: string }>();
    const [open, setOpen] = useState<boolean>(false);
    const waitTime = (time: number = 100) => {
        return new Promise((resolve) => {
            setTimeout(() => {
                resolve(true);
            }, time);
        });
    };

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


