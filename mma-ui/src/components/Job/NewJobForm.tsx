import {
    ModalForm,
    ProForm,
    ProFormText,
    ProFormTextArea,
    ProFormDigit,
    ProFormSelect, ProFormSwitch,
    ProFormList,
    ProFormGroup, ProFormDependency,
    ProFormRadio, ProFormFieldSet, ProFormInstance
} from "@ant-design/pro-components";
import {useEffect, useState, useRef} from "react";
import {getJobOptions, submitJob} from "@/services/job";
import {InputRef, message} from "antd";
import {nowTime, showErrorMsg, uuidV4} from "@/utils/format";

export const NewJobForm = (
    props: {
        db: API.DbModel|null,
        jobType: string,
        visible: boolean,
        setVisible: (_: boolean) => void
        tables?: string[]
    },
) => {
    const [jobOpts, setJobOpts] = useState<API.JobOpts>();
    const [jsonMode, setJsonMode] = useState<boolean>(false);
    let jobDefault = {} as API.Job;
    const [job, setJob] = useState<API.Job>(jobDefault);
    const formRef = useRef<ProFormInstance>();
    const inputRef = useRef<InputRef>(null);

    useEffect(() => {
        if (props.db == undefined) {
            return;
        }

        let db = props.db;
        getJobOptions(db.sourceName, db.name).then((res) => {
             setJobOpts(res.data);
            if (job !== undefined) {
                //@ts-ignore
                job.task_type = res.data.defaultTaskType;
            }
        })

        job.source_name = props.db?.sourceName;
        job.db_name = props.db?.name;

        if (props?.tables !== undefined) {
            job.tables = props?.tables ;
        }

        if (props.jobType == "database") {
            job.description = db.name;
        }

        setJob(job);
        formRef.current?.resetFields();
    }, [props.db, props.tables, props.visible])

    let tableEnum: Record<string, string> = {};
    for (let table of jobOpts?.tables || [] ) {
        tableEnum[table] = table;
    }

    let tableListRules = [{ required: false, warningOnly: true, pattern: /^((\w+)\s*,\s*)*(\w+)$/, message: '多个表名之间以逗号(,)分割' }];

    inputRef.current?.focus();

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

                delete jobJson["enable_config"];

                let PF = "partition_filters";

                if (PF in jobJson) {
                    let pfMap = listFieldConverter(jobJson[PF], "srcTable", "partitionFilter");

                    if (Object.keys(pfMap).length > 0) {
                        jobJson[PF] = pfMap;
                    } else {
                        delete jobJson[PF];
                    }
                }

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

                let TABLE_WHITE_LIST = "table_whitelist";
                let TABLE_BLACK_LIST = "table_blacklist";

                if (TABLE_WHITE_LIST in jobJson) {
                    jobJson[TABLE_WHITE_LIST] = jobJson[TABLE_WHITE_LIST].trim().split(/\s*,\s*/);
                }

                if (TABLE_BLACK_LIST in jobJson) {
                    jobJson[TABLE_BLACK_LIST] = jobJson[TABLE_BLACK_LIST].trim().split(/\s*,\s*/);
                }

                let TABLES = "tables";
                if (TABLES in jobJson) {
                    jobJson[TABLES] = jobJson[TABLES].trim().split(/\s*,\s*/);
                }

                jobJson["type"] = props.jobType;
                setJob(jobJson as API.Job);

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
            {/*<ProFormSwitch*/}
            {/*    initialValue={false}*/}
            {/*    label="编辑配置文件"*/}
            {/*    name="enable_config"*/}
            {/*    onChange={(checked, event) => {*/}
            {/*        setJsonMode(checked);*/}
            {/*    }}*/}
            {/*    labelCol={{ span: 3 }}*/}
            {/*    colProps={{offset: 19}}*/}
            {/*/>*/}

            {
                (() => {
                    if (! jsonMode) {
                        return (
                            <>
                                <ProFormText name="description" label="名称:" placeholder="请输入名称"  rules={[{ required: true, message: '请输入名称' }]}   fieldProps={{ref: inputRef}}/>
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
                                {(() => {
                                    if (props.jobType === "database") {
                                        return(<ProForm.Group>
                                                <ProFormRadio.Group
                                                    name="whiteOrBlackList"
                                                    colProps={{
                                                        offset: 3
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
                                                    {({ whiteOrBlackList }) => {
                                                        if (whiteOrBlackList === 'tableBlackList') {
                                                            return <ProFormTextArea
                                                                name="table_blacklist"
                                                                label="table黑名单"
                                                                rules={tableListRules}
                                                            />
                                                        }

                                                        return <ProFormTextArea
                                                            name="table_whitelist"
                                                            label="table白名单"
                                                            rules={tableListRules}
                                                        />
                                                    }}
                                                </ProFormDependency>
                                            </ProForm.Group>
                                        )
                                    } else if (props.jobType == "tables") {
                                        return <ProFormTextArea
                                            name="tables"
                                            label="table列表"
                                            rules={tableListRules}
                                            initialValue={props?.tables?.join(",")}
                                        />
                                    }
                                })()}

                                <ProFormDependency name={['task_type']}>
                                    {({task_type}) => {
                                        if (task_type != 'ODPS') {
                                            return (
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
                                            )
                                        }
                                    }}
                                </ProFormDependency>

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
                                    initialValue={true}
                                    label="增量更新"
                                    name="increment"
                                />
                                <ProFormSwitch
                                    colProps={{
                                        span: 24,
                                    }}
                                    initialValue={false}
                                    label="只迁schema"
                                    name="schema_only"
                                />
                                <ProFormDependency name={['task_type']}>
                                    {({task_type}) => {
                                        if (task_type == "HIVE_MERGED_TRANS" || task_type == "ODPS_MERGED_TRANS") {
                                            return (
                                                <ProFormSwitch
                                                    colProps={{
                                                        span: 24,
                                                    }}
                                                    initialValue={false}
                                                    label="不拆分分区"
                                                    name="no_unmerge_partition"
                                                />
                                            )

                                        }
                                    }}
                                </ProFormDependency>
                                <ProFormList name="partition_filters" label="分区过滤">
                                    <ProFormGroup key="group">
                                        <ProFormSelect showSearch  name="srcTable" valueEnum={tableEnum} colProps={{span: 10}} placeholder="请输入源表" />
                                        <ProFormText name="partitionFilter"   placeholder={"p1 > '2022-08-20' and p2 > 10"} colProps={{span: 10}} />
                                    </ProFormGroup>
                                </ProFormList>
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
                            </>
                        )
                    }

                    return  <ProFormTextArea
                        name="jsonConfig"
                        label="配置文件"
                        // autoSize={true}
                        // rows={50}
                        // autofocus={true}
                        initialValue={JSON.stringify(job, null, 4)}
                    />
                }) ()
            }


        </ModalForm>
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