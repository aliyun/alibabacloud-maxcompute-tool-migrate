import {
    ModalForm,
    ProForm,
    ProFormText,
    ProFormTextArea,
    ProFormDigit,
    ProFormSelect, ProFormSwitch,
    ProFormList,
    ProFormGroup, ProFormDependency,
    ProFormRadio, ProFormInstance
} from "@ant-design/pro-components";
import React, {useEffect, useState, useRef} from "react";
import {getJobOptions, submitJob} from "@/services/job";
import {InputRef, message} from "antd";
import {showErrorMsg} from "@/utils/format";
import {ProFormTimerPicker} from "@/components/TimerPicker";

export const NewJobForm = (
    {open, setOpen, sourceName, dbName, jobType, tables}:
        {
            open: boolean,
            setOpen: (boolean) => void,
            sourceName: string,
            dbName: string,
            jobType: string,
            tables?: string[]
        }
) => {
    const [jobOpts, setJobOpts] = useState<API.JobOpts>();
    const formRef = useRef<ProFormInstance>();
    const inputRef = useRef<InputRef>();

    console.log("tables", tables);
    formRef.current?.resetFields();

    useEffect(() => {
        getJobOptions(sourceName, dbName).then((res) => {
            setJobOpts(res.data);
        })

    }, [sourceName, dbName, open]);

    const tableEnum: Record<string, string> = {};
    for (const table of jobOpts?.tables || [] ) {
        tableEnum[table] = table;
    }

    const labelCol = 3;
    const onFinish = async (values) => {
        const jobJson = {} as (Record<string, any>);
        Object.assign(jobJson, values);

        delete jobJson["enable_config"];

        const PF = "partition_filters";

        if (PF in jobJson) {
            const pfMap = listFieldConverter(jobJson[PF], "srcTable", "partitionFilter");

            if (Object.keys(pfMap).length > 0) {
                jobJson[PF] = pfMap;
            } else {
                delete jobJson[PF];
            }
        }

        const TABLE_MAPPING = "table_mapping";
        if (TABLE_MAPPING in jobJson) {
            const tableMap = listFieldConverter(jobJson[TABLE_MAPPING], "srcTable", "dstTable");

            if (Object.keys(tableMap).length > 0) {
                jobJson[TABLE_MAPPING] = tableMap;
            } else {
                delete jobJson[TABLE_MAPPING];
            }
        }

        const COLUMN_MAPPING = "column_mapping";
        if (COLUMN_MAPPING in jobJson) {
            const columnMap = listFieldConverter(jobJson[COLUMN_MAPPING], "srcColumn", "dstColumn");

            if (Object.keys(columnMap).length > 0) {
                jobJson[COLUMN_MAPPING] = columnMap;
            } else {
                delete jobJson[COLUMN_MAPPING];
            }
        }

        const TABLE_WHITE_LIST = "table_whitelist";
        const TABLE_BLACK_LIST = "table_blacklist";

        if (TABLE_WHITE_LIST in jobJson) {
            jobJson[TABLE_WHITE_LIST] = jobJson[TABLE_WHITE_LIST].trim().split(/\s*,\s*/);
        }

        if (TABLE_BLACK_LIST in jobJson) {
            jobJson[TABLE_BLACK_LIST] = jobJson[TABLE_BLACK_LIST].trim().split(/\s*,\s*/);
        }

        const TABLES = "tables";
        if (TABLES in jobJson) {
            jobJson[TABLES] = jobJson[TABLES].trim().split(/\s*,\s*/);
        }

        jobJson["type"] = jobType;
        "whiteOrBlackList" in jobJson && delete jobJson["whiteOrBlackList"];

        const hide = message.loading("提交迁移任务中...")
        submitJob(jobJson as API.Job)
            .then((res) => {
                if (res.success) {
                    setOpen(false);
                    return true;
                }

                showErrorMsg(res);
            })
            .catch((e) => {
                const res = e.response.data;
                message.error(res.message, 10);
            })
            .finally(() => {
                hide();
            });
    }

    inputRef.current?.focus();

    return (
        <ModalForm<Record<string, any>>
            title="新建迁移任务"
            layout="horizontal"
            grid={true}
            labelCol={{span: labelCol}}
            colProps={{span: 23}}
            labelAlign="right"
            rowProps={{
                gutter: [16, 16],
            }}
            onFinish={onFinish}
            formRef={formRef}
            open={open}
            onOpenChange={setOpen}
        >
            <ProFormText name="description" label="名称:" placeholder="请输入名称"  rules={[{ required: true, message: '请输入名称' }]} fieldRef={inputRef} />
            <ProFormText name="source_name" label="数据源:" placeholder="请输入名称" initialValue={sourceName} disabled />
            <ProFormText name="db_name" label="库名:" placeholder="请输入名称" initialValue={dbName} disabled />
            <ProFormSelect

                label="任务类型:"
                name="task_type"
                valueEnum={jobOpts?.taskTypes}
                rules={[{ required: true, message: '请选择任务类型' }]}
            />
            <ProFormSelect

                label="MC项目:"
                name="dst_mc_project"
                valueEnum={ (() => {
                    const enums: Record<string, string> = {};

                    for (const mcProject of jobOpts?.dstMcProjects || [] ) {
                        enums[mcProject] = mcProject;
                    }

                    return enums;
                })()}
                rules={[{ required: true, message: '请选择mc项目' }]}
            />
            <ProFormText name="dst_mc_schema" label="MC Schema:" placeholder="请输入mc schema名称"/>
            <JobTypeRender tables={tables} jobType={jobType} labelCol={labelCol} />

            <ProFormSwitch

                initialValue={true}
                label="只迁新分区"
                name="increment"
            />

            <ProFormTimerPicker label="定时执行" name="timer" />

            <ProFormSwitch

                initialValue={true}
                label="开启校验"
                name="enable_verification"
            />

            <ProFormDependency name={['task_type']}>
                {({task_type}) => {
                    if (task_type != 'ODPS') {
                        return (
                            <ProForm.Group>
                                <ProFormSwitch
                                    //colProps={{ md: 12, xl: 8 }}
                                    initialValue={false}
                                    label="合并分区"
                                    name="merge_partition_enabled"
                                />

                                <ProFormDependency name={['merge_partition_enabled']}>
                                    {({ merge_partition_enabled }) => {
                                        if (merge_partition_enabled) {
                                            return (
                                                <ProFormDigit
                                                    label="最大分区层数"
                                                    name="max_partition_level"
                                                    min={0}
                                                    max={10}
                                                    fieldProps={{ precision: 0 }}
                                                    width="xs"
                                                />
                                            )
                                        } else {
                                            return <></>
                                        }
                                    }}
                                </ProFormDependency>
                            </ProForm.Group>
                        )
                    }
                }}
            </ProFormDependency>

            <ProFormList name="partition_filters" label="分区过滤" >
                <ProFormGroup key="group">
                    <ProFormSelect showSearch  name="srcTable" valueEnum={tableEnum}  placeholder="请输入源表" colProps={{span: 10}}  />
                    <ProFormText name="partitionFilter"   placeholder={"p1 > '2022-08-20' and p2 > 10"}  colProps={{span: 10}}  />
                </ProFormGroup>
            </ProFormList>
            <ProFormList name="table_mapping" label="表名映射">
                <ProFormGroup key="group">
                    <ProFormSelect showSearch   name="srcTable" valueEnum={tableEnum} placeholder="请输入源表" colProps={{span: 10}}  />
                    <ProFormText name="dstTable" placeholder="请输入目的表" colProps={{span: 10}}  />
                </ProFormGroup>
            </ProFormList>
            <ProFormList name="column_mapping" label="列名映射">
                <ProFormGroup key="group">
                    <ProFormText name="srcColumn" placeholder="请输入源列名" colProps={{span: 10}}  />
                    <ProFormText name="dstColumn" placeholder="请输入目的列名" colProps={{span: 10}}  />
                </ProFormGroup>
            </ProFormList>
            <ProFormText

                name="table_mapping_pattern"
                label="表名映射规则:"
                placeholder="格式: prefix${table}suffix"
                rules={[{
                    required: false,
                    pattern: /^.*?\$\{table\}.*$/,
                    message: "格式: prefix${table}suffix"
                }]}
            />
        </ModalForm>
    )
}

const JobTypeRender = ({jobType, tables, labelCol}:{jobType: string, tables?: string[], labelCol: number}) => {
    const tableListRules = [{ required: false, warningOnly: true, pattern: /^((\w+)\s*,\s*)*(\w+)$/, message: '多个表名之间以逗号(,)分割' }];

    if (jobType == "database") {
        return <TableWhiteOrBlackList labelCol={labelCol} />
    }

    return <ProFormTextArea
        labelCol={{span: labelCol}}
        name="tables"
        label="table列表"
        rules={tableListRules}
        initialValue={tables?.join(",")}
    />
}

const TableWhiteOrBlackList = ({labelCol}:{labelCol: number} & any) => {
    const tableListRules = [{ required: false, warningOnly: true, pattern: /^((\w+)\s*,\s*)*(\w+)$/, message: '多个表名之间以逗号(,)分割' }];

    return (
        <ProForm.Group>
            <ProFormRadio.Group
                name='whiteOrBlackList'
                colProps={{
                    offset: labelCol,
                }}
                initialValue='tableWhiteList'
                options={[
                    {
                        label: 'table白名单',
                        value: 'tableWhiteList',
                    },
                    {
                        value: 'tableBlackList',
                        label: "table黑名单"
                    },
                ]}
            />

            <ProFormDependency name={['whiteOrBlackList']}>
                {
                    ({whiteOrBlackList}) => {
                        let name="table_whitelist"
                        let label="table白名单"

                        if (whiteOrBlackList === 'tableBlackList') {
                            name = "table_blacklist"
                            label = "table黑名单"
                        }

                        return <ProFormTextArea
                            name={name}
                            label={label}
                            colProps={{span: 24}}
                            rules={tableListRules}
                        />
                    }
                }
            </ProFormDependency>
        </ProForm.Group>
    )
}

function listFieldConverter(origin: [Record<string, string>], keyKey: string, valueKey: string) {
    let dst =  origin.filter((pf: any) => keyKey in pf && valueKey in pf);

    let kvMap = {} as (Record<string, string>);
    for (let kv of dst) {
        kvMap[kv[keyKey]] = kv[valueKey];
    }

    return kvMap;
}