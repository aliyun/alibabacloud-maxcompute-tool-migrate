import { request } from "@umijs/max";
import {getLocale} from "umi";

export async function getMMAConfig(): Promise<API.MMARes<API.MMConfig>>  {
    return  await request<API.MMARes<API.MMConfig>>(
        "/api/config",
        {
            method: "GET",
            params: {lang: getLocale()}
        }
    );
}

export async function saveMMAConfig(body: API.MMAConfigJson) {
    return request<API.MMARes<any>>("/api/config", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        data: body
    });
}

export async function updateMMAConfig(body: API.MMAConfigJson) {
    return request<API.MMARes<any>>("/api/config", {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
        },
        data: body
    });
}

export async function checkConfigStatus() {
    return request<API.MMARes<{inited: boolean}>>(
        "/api/config/status",
        {
            method: "GET"
        }
    )
}

export async function restartMMA() {
    return request(
        "/api/restart",
        {
            method: "PUT"
        }
    )
}

export async function pingMMA() {
    return request<string>(
        "/api/ping",
        {
            method: "GET"
        }
    )
}

export async function getDstMcProjects() {
    return request<API.MMARes<[string]>>(
        "/api/config/dstMcProjects",
        {
            method: "GET"
        }
    )
}

export async function getMMAVersion() {
    return request<string>(
        "/api/version",
        {
            method: "GET"
        }
    )
}

export default {
    getMMAConfig,
    saveMMAConfig,
}