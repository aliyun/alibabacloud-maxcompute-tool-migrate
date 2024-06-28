import { request } from "@umijs/max";
import {paramsSerializer} from "@/utils/format";
import {SortOrder} from "antd/lib/table/interface";
import React from "react";

export async function getJobs(
    params: any & { pageSize?: number; current?: number; keyword?: string },
    sort: Record<string, SortOrder>,
    filter: Record<string, React.ReactText[] | null>
) {
    return request<API.MMARes<API.Job[]>>(
        "/api/jobs",
        {
            method: "GET",
            params: {...params},
            paramsSerializer: paramsSerializer
        }
    )
}

export async function getJobBatches(jobId: number): Promise<API.MMARes<API.JobBatch[]>>{
    return request<API.MMARes<API.JobBatch[]>>(
        `/api/jobs/${jobId}/batches`,
        {
            method: "GET"
        }
    );
}

export async function getJobOptions(dsName: string, dbName: string) {
    return request<API.MMARes<API.JobOpts>>(
        "/api/jobs/options",
        {
            method: "GET",
            params: {
                ds_name: dsName,
                db_name: dbName,
            }
        }
    )
}

export async function submitJob(job: API.Job) {
    return request<API.MMARes<any>>("/api/jobs", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        data: job
    });
}

export async function jobAction(jobId: number, action: string) {
    return request<API.MMARes<any>>(`/api/jobs/${jobId}?action=${action}`, {
        method: 'PUT',
    });
}

export async function getJobBasicInfo(): Promise<API.MMARes<API.IdToName>> {
    return request<API.MMARes<API.IdToName>>(`/api/jobs/basic`, {
        method: 'GET',
    });
}

export async function getTasks(
    params: any & { pageSize?: number; current?: number; keyword?: string },
    sort: Record<string, SortOrder>,
    filter: Record<string, React.ReactText[] | null>
) {
    let {...data}: any = params;
    data["sorter"] = sort;
    return request<API.MMARes<API.Task[]>>("/api/tasks", {
        method: "PUT",
        headers: {
            'Content-Type': 'application/json',
        },
        data: data
    })
}

export async function getTaskLog(taskId: number) {
    return request<API.MMARes<API.TaskLog[]>>(
        `/api/tasks/${taskId}`,
        {
            method: "GET",
        }
    )
}

export async function downloadTaskLog(taskId: number) {
    return request<API.MMARes<API.TaskLog[]>>(
        `/api/tasks/${taskId}/logs`,
        {
            method: "GET",
        }
    )
}

export async function getTaskPartitions(taskId: number) {
    return request<API.MMARes<API.PartitionModel[]>>(
        `/api/tasks/${taskId}/partitions`,
        {
            method: "GET",
        }
    )
}

export async function getTaskTypeMap() {
    return request<API.MMARes<Record<string, string>>>(
        '/api/tasks/typeNames',
        {
            method: "GET",
        }
    )
}