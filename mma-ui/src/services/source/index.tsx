import { request } from "@umijs/max";
import {SortOrder} from "antd/lib/table/interface";
import {getLocale} from "umi";

export async function getSources() {
    return request<API.MMARes<API.DataSource[]>>("/api/sources", {
        method: 'GET'
    });
}

export async function getSourceByName(sourceName: string, withConfig: boolean) {
    let url = `/api/sources/byName?name=${sourceName}`;
    if (withConfig) {
        url = `${url}&config=1`
    }

    return request<API.MMARes<API.DataSource>>(url, {
        method: 'GET'
    })
}

export async function getSource(sourceId: string) {
    return request<API.MMARes<API.DataSource>>("/api/sources/" + sourceId, {
        method: 'GET'
    })
}

export async function updateSource(sourceId: number) {
    return request("/api/sources/" + sourceId + "/update", {
        method: 'PUT'
    });
}

export async function runSourceInitializer(sourceId: number) {
    return request<API.MMARes<any>>("/api/sources/" + sourceId + "/init", {
        method: 'PUT'
    });
}

export async function addSource(body: API.MMAConfigJson) {
    return request<API.MMARes<{ name: string, id: number }>>("/api/sources", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        data: body
    });
}


export async function updateSourceConfig(sourceId: number, body: API.MMAConfigJson) {
    return request<API.MMARes<any>>(`/api/sources/${sourceId}`, {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        data: body
    });
}

export async function getSourceConfig(sourceName: string)  {
    return await request<API.MMARes<API.MMConfig>>(
        `/api/sources/${sourceName}/config`,
        {
            method: "GET",
            params: {lang: getLocale()}
        }
    );
}

export async function  getActionLogs(sourceId: number, actionType: string) {
    return request<API.MMARes<API.ActionLog[]>>(`/api/sources/${sourceId}/action_logs`,
        {
            method: 'GET',
            params: {'actionType': actionType}
        }
    )
}

export async function getSourceItems(type: string) {
    return request<API.MMARes<API.ConfigItem[]>>(`/api/sources/items/`, {
        method: 'GET',
        params: {type: type, lang: getLocale()}
    });
}

export async function getSourceTypes() {
    return request<API.MMARes<string[]>>('/api/sources/types', {
        method: 'GET',
    });
}

export async function getLoadingMetaProgress(sourceId: number): Promise<{progress: number} & API.MMARes<any>> {
    return request("/api/sources/" + sourceId + "/progress", {
        method: 'GET'
    });
}

export async function getDbs(
    params: {id?: number, page?: number, pageSize?: number, sourceId?: number, status?: string,  name?: string},
    sort?: Record<string, SortOrder>,
    filter?: Record<string, (string | number)[]|null>
) {
    let {...data}: any = params;
    data["sorter"] = sort;

    return request<API.MMARes<API.DbModel[]>>("/api/dbs", {
        method: "PUT",
        headers: {
            'Content-Type': 'application/json',
        },
        data: data
    })
}

export async function getTables(
    params: {page?: number, pageSize?: number, sourceId?: number, status?: string[]},
    sort?: Record<string, SortOrder>,
    filter?: Record<string, (string | number)[]|null>
) {
    let {...data}: any = params;
    data["sorter"] = sort;

    return request<API.MMARes<API.TableModel[]>>("/api/tables", {
        method: "PUT",
        headers: {
            'Content-Type': 'application/json',
        },
        data: data
    })
}

export async function getPts(
    params: {page?: number, pageSize?: number, sourceId?: number, status?: string[]},
    sort?: Record<string, SortOrder>,
    filter?: Record<string, (string | number)[]|null>
) {
    let {...data}: any = params;
    data["sorter"] = sort;
    return request<API.MMARes<API.PartitionModel[]>>("/api/partitions", {
        method: "PUT",
        headers: {
            'Content-Type': 'application/json',
        },
        data: data
    })
}

export async function resetPts(ptIds: number[]) {
    return request<API.MMARes<number>>("/api/partitions/status", {
        method: "PUT",
        headers: {
            'Content-Type': 'application/json',
        },
        data: {ptIds: ptIds}
    })
}