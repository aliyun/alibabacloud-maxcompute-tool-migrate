import {useEffect, useRef, useState} from "react";
import {
    ProFormInstance,
    ModalForm,
    ProFormText,
    ProFormSelect,
    ProFormSwitch,
    ProFormList, ProFormGroup, ProFormTextArea, ProForm, ProFormDependency, ProFormDigit
} from "@ant-design/pro-components";
import {getJobOptions, submitJob} from "@/services/job";
import {nowTime, showErrorMsg} from "@/utils/format";
import {message} from "antd";

export const NewPartitionJobForm = (
    props: {
        db: API.DbModel|null,
        visible: boolean,
        setVisible: (_: boolean) => void
        partitions: API.PartitionModel[],
    },
) => {
    const [jobOpts, setJobOpts] = useState<API.JobOpts>();
    const formRef = useRef<ProFormInstance>();

    useEffect(() => {
        if (props.db == undefined) {
            return;
        }

        let db = props.db;
        getJobOptions(db.sourceName, db.name).then((res) => {
            setJobOpts(res.data);
        })


        formRef.current?.resetFields();
    }, [props.db, props.visible]);

    let tableEnum: Record<string, string> = {};
    for (let table of jobOpts?.tables || [] ) {
        tableEnum[table] = table;
    }

    let ptValues = props.partitions.map(p => `${p.tableName}.${p.value}`).join("\n");

    return (
        <ModalForm<Record<string, any>>
            title={"新建迁移任务"}
            visible={props.visible}
            onVisibleChange={props.setVisible}
            layout={"horizontal"}
            grid={true}
            // rowProps={{
            //     gutter: [16, 0],
            // }}
            labelCol={{ span: 3 }}
            labelAlign="left"
            formRef={formRef}
            onFinish={async (values) => {
                let jobJson = {} as (Record<string, any>);
                Object.assign(jobJson, values);

                let TABLE_MAPPING = "table_mapping";
                if (TABLE_MAPPING in jobJson) {
                    let tableMap = listFieldConverter(jobJson[TABLE_MAPPING], "srcTable", "dstTable");

                    if (Object.keys(tableMap).length > 0) {
                        jobJson[TABLE_MAPPING] = tableMap;
                    } else {
                        delete jobJson[TABLE_MAPPING];
                    }
                }

                let COLUMN_MAPPING = "column_mapping";
                if (COLUMN_MAPPING in jobJson) {
                    let columnMap = listFieldConverter(jobJson[COLUMN_MAPPING], "srcColumn", "dstColumn");

                    if (Object.keys(columnMap).length > 0) {
                        jobJson[COLUMN_MAPPING] = columnMap;
                    } else {
                        delete jobJson[COLUMN_MAPPING];
                    }
                }

                jobJson["type"] = "partitions";
                jobJson["increment"] = false;
                jobJson["partitions"] = props.partitions.map(p => p.id);

                let hide = message.loading("提交迁移任务中...")

                submitJob(jobJson as API.Job)
                    .then((res) => {
                        if (res.success) {
                            props.setVisible(false);
                            return;
                        }

                        showErrorMsg(res);
                    })
                    .catch((e) => {
                        let res = e.response.data;
                        message.error(res.message, 10);
                    })
                    .finally(() => {
                        hide();
                    });
            }}
        >
            <ProFormText name="description" label="名称:" placeholder="请输入名称" rules={[{ required: true, message: '请输入名称' }]} />
            <ProFormText name="source_name" label="数据源:" placeholder="请输入名称" initialValue={props.db?.sourceName} disabled />
            <ProFormText name="db_name" label="库名:" placeholder="请输入名称" initialValue={props.db?.name} disabled />
            <ProFormSelect
                colProps={{span: 24}}
                label="任务类型:"
                name="task_type"
                initialValue={jobOpts?.defaultTaskType}
                valueEnum={jobOpts?.taskTypes}
                rules={[{ required: true, message: '请选择任务类型' }]}
            />
            <ProFormSelect
                colProps={{span: 24}}
                label="mc项目:"
                name="dst_mc_project"
                valueEnum={ (() => {
                    let enums: Record<string, string> = {};

                    for (let mcProject of jobOpts?.dstMcProjects || [] ) {
                        enums[mcProject] = mcProject;
                    }

                    return enums;
                })()}
                rules={[{ required: true, message: '请选择mc项目' }]}
            />

            <ProForm.Group labelLayout='inline'>
                <ProFormSwitch
                    labelCol={{ span: 12 }}
                    //colProps={{ md: 12, xl: 8 }}
                    colProps={{ span: 6 }}
                    initialValue={false}
                    label="合并分区"
                    name="merge_partition_enabled"
                />

                <ProFormDependency name={['merge_partition_enabled']}>
                    {({ merge_partition_enabled }) => {
                        if (merge_partition_enabled) {
                            return (
                                <ProFormDigit
                                    labelCol={{ span: 12 }}
                                    colProps={{ span: 8 }}
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

            <ProFormSwitch
                colProps={{
                    span: 24,
                }}
                initialValue={true}
                label="开启校验"
                name="enable_verification"
            />
            <ProFormSwitch
                colProps={{
                    span: 24,
                }}
                initialValue={false}
                label="只迁schema"
                name="schema_only"
            />
            <ProFormTextArea
                name="ptValues"
                label="partition列表"
                initialValue={ptValues}
                disabled
            />
            <ProFormList name="table_mapping" label="表名映射">
                <ProFormGroup key="group">
                    <ProFormSelect showSearch   name="srcTable" valueEnum={tableEnum} colProps={{span: 10}} placeholder="请输入源表" />
                    <ProFormText name="dstTable"  colProps={{span: 10}} placeholder="请输入目的表" />
                </ProFormGroup>
            </ProFormList>
            <ProFormList name="column_mapping" label="列名映射">
                <ProFormGroup key="group">
                    <ProFormText name="srcColumn"  colProps={{span: 10}} placeholder="请输入源列名" />
                    <ProFormText name="dstColumn"  colProps={{span: 10}} placeholder="请输入目的列名" />
                </ProFormGroup>
            </ProFormList>
        </ModalForm>
    );
}

function listFieldConverter(origin: [Record<string, string>], keyKey: string, valueKey: string) {
    let dst =  origin.filter((pf: any) => keyKey in pf && valueKey in pf);

    let kvMap = {} as (Record<string, string>);
    for (let kv of dst) {
        kvMap[kv[keyKey]] = kv[valueKey];
    }

    return kvMap;
}