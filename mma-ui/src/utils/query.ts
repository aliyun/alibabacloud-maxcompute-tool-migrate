

export const processStatus = (params: Record<string, any>,  filter?: Record<string, (string | number)[]|null>) => {
    let statuses: [string] = (filter?.status as [string]) || [];
    let status = params.status || "all";

    if (status != "all" && statuses.indexOf(status) < 0) {
        statuses.push(status);
    }

    delete params["status"];
    params["statusList"] = statuses;

    return params;
}