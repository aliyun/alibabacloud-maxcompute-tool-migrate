import {
    PageContainer,
    StepsForm,
    ProFormInstance,
    ProFormText,
    ProFormCheckbox,
    ProForm,
    ProFormSelect, ProFormTextArea, ProFormSwitch, ProFormDigit
} from "@ant-design/pro-components";
import {addSource, getSource, getSourceItems, getSourceTypes} from "@/services/source";
import {useEffect, useRef, useState} from "react";
import {SourceLoadingProgress} from "@/components/Source/SourceLoadingProgress";
import {showErrorMsg} from "@/utils/format";
import * as React from "react";
import {history, useIntl} from "umi";
import {SOURCES_ROUTE} from "@/constant";
import {message, Form} from "antd";
import {ProFormTimerPicker, TimerPicker} from "@/components/TimerPicker";
import {FMSpan, FM, fm} from "@/components/i18n";

export default () => {
    const formRef = useRef<ProFormInstance>();
    const [allSourceTypes, setAllSourceTypes] = useState<Record<string, string>>();
    const [sourceType, setSourceType] = useState<string>("HIVE");
    const [configItems, setConfigItems] = useState<API.ConfigItem[]>([]);
    const [newSource, setNewSource] = useState<API.DataSource>();
    const [visible, setVisible] = useState(false);
    const intl = useIntl();

    useEffect(() => {
        getSourceTypes().then((res) => {
            let sources = res?.data || [];
            let sourceMap = {} as Record<string, string>;

            for (let s of sources) {
                if (s == "ODPS") {
                    sourceMap[s] = "MAXCOMPUTE";
                } else {
                    sourceMap[s] = s;
                }
            }

            setAllSourceTypes(sourceMap);
        })
    }, []);


    return (
        <PageContainer>
            <StepsForm<{
                name: string;
            }>
                formRef={formRef}
                onFinish={async () => {

                }}
                formProps={{
                    validateMessages: {
                        required: fm(intl, "pages.Source.NewSource.required", '此项为必填项'),
                    },
                }}
            >
                <StepsForm.StepForm<{
                    name: string;
                }>
                    key="step1"
                    name="base"
                    title={fm(intl, "pages.Source.NewSource.selectType", "选择数据源类型")}
                    onFinish={async () => {
                        let type = formRef.current?.getFieldsValue().type
                        let res = await getSourceItems(type);
                        setConfigItems(res?.data || []);
                        setSourceType(type);
                        return true;
                    }}
                >
                    <ProFormSelect
                        colProps={{span: 24}}
                        label={fm(intl, "pages.Source.NewSource.sourceType",  "数据源类型:")}
                        name="type"
                        valueEnum={allSourceTypes}
                        rules={[{ required: true, message: fm(intl, "pages.Source.NewSource.selectType", "选择数据源类型") }]}
                    />

                </StepsForm.StepForm>
                <StepsForm.StepForm
                    key="step2"
                    name="SourceConfig"
                    title={fm(intl, "pages.Source.NewSource.configure", "配置数据源")}
                    onFinish={async () => {
                        let values = formRef.current?.getFieldsValue();
                        let keyToType = new Map<string, string>();
                        for (let c of configItems) {
                            keyToType.set(c.key, c.type);
                        }

                        let keys = Object.keys(values);
                        for (let key of keys) {
                            let value = values[key];

                            if (value === undefined) {
                                delete values[key];
                                continue;
                            }

                            if (typeof value === 'string') {
                                value = value.trim();
                                values[key] = value;
                            }

                            switch (keyToType.get(key)) {
                                case "map":
                                    values[key] = JSON.parse(value);
                                    break
                                case "list":
                                    if (value != '') {
                                        values[key] = values[key].trim().split(/\s*,\s*/);
                                    } else {
                                        delete values[key];
                                    }
                                    break
                                default:
                                    break
                            }
                        }
                        let hide = message.loading(fm(intl, "pages.Source.NewSource.addingSource", "添加数据源中"));
                        let res =  await addSource(values);
                        hide();
                        if (! res.success) {
                           showErrorMsg(res);
                           return true;
                        }

                        let sourceId = res?.data?.id;
                        if (sourceId !== undefined) {
                            let res1 = await getSource(sourceId.toString());
                            setVisible(true);
                            setNewSource(res1?.data);
                        }

                        return true;
                    }}
                >
                    <SourceConfigForm sourceType={sourceType} configItems={configItems} />
                    <SourceLoadingProgress dataSource={newSource} visible={visible} onCancel={() => {
                        setVisible(false);
                        history.push(SOURCES_ROUTE);
                    }} />
                </StepsForm.StepForm>
            </StepsForm>
        </PageContainer>
    )
}

const SourceConfigForm = (props: {sourceType: string, configItems: API.ConfigItem[]}) => {
    const intl = useIntl();

    let items = props.configItems.map((config, index) => {
        let rules = [];

        if (config.required) {
            rules.push({ required: true, message:  fm(intl, "pages.Source.NewSource.required", '此项为必填项')});
        }

        let itemProps = {
            name: config.key,
            label: config.desc,
            rules: rules,
            initialValue: config.value,
            //tooltip: config.desc,
        }

        if (config.key === "type") {
            itemProps.initialValue = props.sourceType;
        }

        switch (config.type) {
            case "map":
                rules.push({
                    validator: (_: any, value: string) => {
                        try {
                            JSON.parse(value);
                        } catch (e) {
                            return Promise.reject(new Error(fm(intl, "pages.Source.NewSource.invalidJson", '请填入合法的json字符串')));
                        }

                        return Promise.resolve();
                    }
                });

                return <ProFormTextArea
                    {...itemProps}
                    key={index}
                    initialValue={(() => JSON.stringify(config.value, null, 4))()}
                />
            case "boolean":
                return <ProFormSwitch
                    {...itemProps}
                    key={index}
                />;
            case "list":
                rules.push({pattern: /^([\w\\*\\.]+\s*,\s*)*([\w\\*\\.]+)$/, message: fm(intl, "pages.Source.NewSource.invalidList",  '该项值为列表, 值之间请以","分割')});

                return <ProFormTextArea
                    {...itemProps}
                    key={index}
                    initialValue={(() => Array.isArray(config.value) ? (config.value as string[]).join(", ") : "")()}
                />
            case "int":
            case "long":
                return <ProFormDigit {...itemProps} key={index}/>;
            case "password":
                return <ProFormText.Password  {...itemProps} key={index}/>
            case "timer":
                return <ProFormTimerPicker name={itemProps.name} label={itemProps.label}  key={index}/>
            default:
                return <ProFormText {...itemProps} disabled={config.key == "type"} key={index} />;
        }
    })

    return <>{items}</>;
}