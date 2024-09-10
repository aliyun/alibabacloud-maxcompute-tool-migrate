import {
    ActionType, EditableFormInstance,
    EditableProTable,
    FormInstance,
    ProColumns, ProConfigProvider, ProForm,
    ProRenderFieldPropsType
} from "@ant-design/pro-components";
import React, {useEffect, useRef, useState} from "react";
import {Button, message} from "antd";
import {showErrorMsg} from "@/utils/format";
import {TimerPicker} from "@/components/TimerPicker";
import {useIntl} from "umi";
import {FMSpan, fm} from "@/components/i18n";
import "./Config.css";

const valueTypeMap: Record<string, ProRenderFieldPropsType> = {
    timer: {
        renderFormItem(text, props) {
            return (
                <ProForm.Item style={{margin: "0px"}}>
                    <TimerPicker  {...props.fieldProps} />
                </ProForm.Item>
            );
        },
    }
};

const ConfigTable = (props: {request: () => Promise<API.MMARes<API.ConfigItem[]>>, onSave: (c: API.MMAConfigJson) => Promise<API.MMARes<any>>, afterSave?: (_?: any) => any}) => {
    const [editableKeys, setEditableRowKeys] = useState<React.Key[]>();
    const [configValue, setConfigValue] = useState<API.ConfigItem[]>();
    const aRef = useRef<ActionType>();
    const intl = useIntl();

    useEffect(() => {
        aRef.current?.reset?.();
    }, [props])

    const columns: ProColumns<API.ConfigItem>[] = [
        {
            title: fm(intl, "components.Config.ConfigTable.key", "配置项"),
            dataIndex: 'key',
            editable: false,
            width: '15%',
            render: (dom, entity) => {
                if (entity.required) {
                    return <span className="required-key">{entity.key}</span>;
                }

                return <span >{entity.key}</span>
            }
        },
        {
            title: fm(intl, "components.Config.ConfigTable.value", "配置值"),
            dataIndex: 'value',
            editable: (value: any, record: API.ConfigItem)  => {
                return record.editable?? true;
            },

            valueType: (config, form) => {
                switch (config.type) {
                    case "map":
                        return "jsonCode";
                    case "boolean":
                        return "switch";
                    case "list":
                        return "textarea";
                    case "password":
                        return "password";
                    case "timer":
                        return "timer";
                    case "int":
                    case "long":
                    default:
                        break
                }

                return "text";
            },

            formItemProps: (form: FormInstance<API.MMConfig>, { rowKey, key, index}) => {
                if (rowKey === undefined) {
                    return {
                        rules: [{ required: true, message: fm(intl, 'components.Config.ConfigTable.required', '此项为必填项') }]
                    };
                }

                let config = form.getFieldValue(rowKey);

                let rules = [];
                if (config?.required) {
                    rules.push({ required: true, message: fm(intl, 'components.Config.ConfigTable.required', '此项为必填项') });
                }

                if (config?.type === "int") {
                    rules.push({pattern: /[1-9][0-9]*$/, message: fm(intl, 'components.Config.ConfigTable.needNum', '该项值为数字')});
                }

                if (config?.type === "list") {
                    rules.push({pattern: /^([\w]+\s*,\s*)*([\w]+)$/, message:  fm(intl, 'components.Config.ConfigTable.invalidList',  '该项值为列表，值之间请以","分割')});
                }

                if (config?.type === "map") {
                    rules.push( {
                        validator: (_: any, value: string) => {
                            try {
                                JSON.parse(value);
                            } catch (e) {
                                return Promise.reject(new Error(fm(intl, 'components.Config.ConfigTable.invalidJson',  '请填入合法的json字符串')));
                            }

                            return Promise.resolve();
                        }
                    });
                }

                return {
                    rules: rules
                };
            },

        },
        {
            title: fm(intl, "components.Config.ConfigTable.desc", "描述"),
            dataIndex: 'desc',
            editable: false
        }
    ];


    return (
        <ProConfigProvider valueTypeMap={valueTypeMap}>
        <EditableProTable<API.ConfigItem>
            rowKey="key"
            columns={columns}
            value={configValue}
            controlled={true}
            onChange={(configItems: API.ConfigItem[]) => {
                if (configItems === undefined) {
                    return;
                }

                for (const item of configItems) {
                    if (typeof item.value !== 'string') {
                        continue;
                    }

                    item.value = (item.value as string).trim();

                    if (item.type === "list" && item.value != undefined) {
                        let value = (item.value as string);

                        if (value === "") {
                            item.value = [];
                        } else {
                            item.value = (item.value as string).split(/\s*,\s*/);
                        }
                    }
                }

                setConfigValue(configItems);
            }}
            request={props.request}
            postData={(data: API.MMConfig) => {
                setEditableRowKeys(data.map(c=>c.key));
                for (let config of data) {
                    if (config.type === "map") {
                        config.value = JSON.stringify(config.value, null, 4);
                    }
                }
                return data;
            }}
            actionRef={aRef}
            //formRef={formRef}
            toolBarRender={() => {
                return [
                    <Button
                        type="primary"
                        key="save"
                        onClick={async () => {
                            if (configValue === undefined) {
                                return;
                            }
                            let configJson: API.MMAConfigJson = {};
                            for (const configItem of configValue) {
                                if (configItem.type == "map") {
                                    configJson[configItem.key] = JSON.parse(configItem.value as string);
                                } else {
                                    configJson[configItem.key] = configItem.value
                                }
                            }

                            const hide = message.loading(fm(intl, "components.Config.ConfigTable.saving", "正在保存..."));

                            try {
                                let res = await props.onSave(configJson)
                                hide();
                                if (res?.success == true) {
                                    message.success("success", 5);

                                    if (props?.afterSave != undefined) {
                                        props?.afterSave();
                                    }

                                    return;
                                }

                                showErrorMsg(res);
                            } catch (e) {
                                hide();

                                message.error(fm(intl, "components.Config.ConfigTable.failed", "发生错误..."), 10);
                            }
                        }}
                    >
                        <FMSpan id="components.Config.ConfigTable.save" defaultMessage="保存" />
                    </Button>,

                    <Button
                        key="reset"
                        onClick={() => {
                            aRef.current?.reload();
                        }}
                    >
                        <FMSpan id="components.Config.ConfigTable.reset" defaultMessage="重置" />
                    </Button>
                ];
            }}
            recordCreatorProps={false}
            editable={{
                type: 'multiple',
                editableKeys,
                onValuesChange: (record, recordList) => {
                    setConfigValue(recordList);
                },
                onChange: setEditableRowKeys,
            }}
        />
        </ProConfigProvider>
    )
};

export default ConfigTable;