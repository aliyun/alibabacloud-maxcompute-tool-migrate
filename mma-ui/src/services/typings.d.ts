declare namespace API {
    type ConfigValue = string | number | string[] | number[] | Map<String, any>;

    interface ConfigItem {
        type: string,
        key: string,
        value: ConfigValue,
        required?: boolean,
        desc: string,
        name?: string,
        password?: boolean
    }

    type MMConfig = ConfigItem[];
    type MMAConfigJson = { [key: string]: ConfigValue };
    type MMARes<T> = {
        success: boolean,
        message: string,
        errors?: {
            [key: string]: string
        }
        total?: number,
        data?: T
    }

    type DataSource = {
        id: number,
        name: string,
        type: string,
        lastUpdateTime: string,
        dbNum: number,
        tableNum: number,
        partitionNum: number,
        config: ConfigItem[]
    }

    type DbModel = {
        "id": number,
        "name": string,
        "sourceId": number,
        "size": number,
        "numRows": number,
        "updated": boolean,
        "lastDdlTime": string,
        "createTime": string,
        "updateTime": string,
        "status": string,
        "sourceName": string,
        "description": string,
        "tables": number,
        "tablesDoing": number,
        "tablesDone": number,
        "tablesPartDone": number,
        "tablesFailed": number,
        "partitions": number,
        "partitionsDoing": number,
        "partitionsDone": number,
        "partitionsFailed": number,
    }

    type TableModel = {
        "id": number,
        "dbId": 1,
        "sourceId": number,
        "size": number,
        "numRows": number,
        "lastDdlTime": string,
        "updated": boolean,
        "createTime": string,
        "updateTime": string,
        "status": string,
        "sourceName": string,
        "dbName": string,
        "name": string,
        "type": string,
        "hasPartitions": boolean,
        "partitions": number,
        "partitionsDoing": number,
        "partitionsDone": number,
        "partitionsFailed": number,
        "schema": {
            "name": string,
            "columns": [
                {
                    "name": string,
                    "type": string,
                    "comment": string,
                    "nullable": boolean
                }
            ],
            "partitions": [
                {
                    "name": string,
                    "type": string,
                    "comment": string,
                    "nullable": boolean
                }
            ]
        }
    };

    type PartitionModel = {
        "id": number,
        "value": string,
        "sourceId": number,
        "dbId": number,
        "tableId": number
        "dbName": string,
        "tableName": string,
        "size": number,
        "numRows": number,
        "updated": boolean,
        "lastDdlTime": string,
        "createTime": string,
        "updateTime": string,
        "status": string,
        "sourceName": string,
    };

    type Job = {
        "id": number,
        "source_name": string,
        "db_name": string,
        "dst_odps_name": string,
        "description": string,
        "status": string,
        "type": string,
        "stopped": boolean,
        "restart": boolean,
        "deleted": boolean,
        "tables": string[],
        "task_type": string,
        "partition_filters": Record<string, string>,
        "increment": boolean,
        "enable_verification": boolean,
        "createTime": string,
        "updateTime": string,
        "config": any,
    }

    type JobOpts = {
        dstMcProjects: [string],
        taskTypes: {string: string},
        defaultTaskType: string,
        tables: [string],
    }

    type Task =     {
        "id": number,
        "jobId": number,
        "sourceId": number,
        "dbId": number,
        "tableId": number,
        "sourceName": string,
        "dbName": string,
        "tableName": string,
        "odpsProject": string,
        "odpsTable": string,
        "type": string,
        "status": string,
        "stopped":boolean,
        "restart": boolean,
        "retriedTimes": number,
        "startTime": string,
        "endTime": string,
        "createTime": string,
        "updateTime": string
    }

    type TaskLog = {
        "id": number,
        "status": string,
        "action": string,
        "msg": string,
        "createTime": string,
        "updateTime": string,
    }

    type IdToName = Record<number, string>;

    interface PageInfo {
        current?: number;
        pageSize?: number;
        total?: number;
        list?: Array<Record<string, any>>;
    }

    interface PageInfo_UserInfo_ {
        /**
         1 */
        current?: number;
        pageSize?: number;
        total?: number;
        list?: Array<UserInfo>;
    }

    interface Result {
        success?: boolean;
        errorMessage?: string;
        data?: Record<string, any>;
    }

    interface Result_PageInfo_UserInfo__ {
        success?: boolean;
        errorMessage?: string;
        data?: PageInfo_UserInfo_;
    }

    interface Result_UserInfo_ {
        success?: boolean;
        errorMessage?: string;
        data?: UserInfo;
    }

    interface Result_string_ {
        success?: boolean;
        errorMessage?: string;
        data?: string;
    }

    type UserGenderEnum = 'MALE' | 'FEMALE';

    interface UserInfo {
        id?: string;
        name?: string;
        /** nick */
        nickName?: string;
        /** email */
        email?: string;
        gender?: UserGenderEnum;
    }

    interface UserInfoVO {
        name?: string;
        /** nick */
        nickName?: string;
        /** email */
        email?: string;
    }

    type definitions_0 = null;
}
