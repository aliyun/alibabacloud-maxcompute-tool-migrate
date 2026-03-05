import {AliyunOutlined} from '@ant-design/icons';
import {ProBreadcrumb, RouteContext} from "@ant-design/pro-components";
import {Route} from "antd/lib/breadcrumb/Breadcrumb";
import React, {useContext} from "react";
import {Breadcrumb, BreadcrumbProps, Image} from "antd";
import {checkConfigStatus} from "@/services/config"
import {history} from '@umijs/max';
import type {RunTimeLayoutConfig} from '@umijs/max';
import {SelectLang} from "@/SelectLang";
import {setLocale} from "umi";
import Cookies from 'js-cookie'

const configPath = '/config';

const SvgMaxcompute = (props) => (
    <svg
        xmlns="http://www.w3.org/2000/svg"
        width="1em"
        height="1em"
        fill="currentColor"
        className="maxcompute_svg__icon"
        viewBox="0 0 1024 1024"
        {...props}
    >
        <path d="M896 576c-12.3 0-24.2 1.8-35.5 5L708.1 284.7C744.6 255.4 768 210.4 768 160 768 71.8 696.2 0 608 0S448 71.8 448 160c0 64.1 37.9 119.6 92.6 145.1l-79.8 335.4c-4.2-.3-8.4-.4-12.7-.4-39.6 0-76.4 12-107 32.6l-98.4-103.8c8.5-17.1 13.3-36.4 13.3-56.8 0-70.6-57.4-128-128-128S0 441.4 0 512s57.4 128 128 128c26.9 0 51.9-8.4 72.5-22.6l94.1 99.3C270.4 748.8 256 788.8 256 832c0 105.9 86.1 192 192 192s192-86.1 192-192c0-79.3-48.3-147.5-117-176.8l79.8-335.3c1.7.1 3.4.1 5.2.1 15 0 29.5-2.1 43.2-5.9l154.1 299.7c-23 23.1-37.3 55-37.3 90.2 0 70.6 57.4 128 128 128s128-57.4 128-128-57.4-128-128-128m-768 0c-35.3 0-64-28.7-64-64s28.7-64 64-64 64 28.7 64 64-28.7 64-64 64m448 256c0 70.6-57.4 128-128 128s-128-57.4-128-128 57.4-128 128-128 128 57.4 128 128m-64-672c0-52.9 43.1-96 96-96s96 43.1 96 96-43.1 96-96 96-96-43.1-96-96m384 608c-35.3 0-64-28.7-64-64s28.7-64 64-64 64 28.7 64 64-28.7 64-64 64" />
    </svg>
);

// 运行时配置

// 全局初始化数据配置，用于 Layout 用户信息和权限初始化
// 更多信息见文档：https://next.umijs.org/docs/api/runtime-config#getinitialstate
export async function getInitialState(): Promise<{ inited: boolean }> {

    const search = window.location.search;
    const urlParams = new URLSearchParams(search);
    const urlLang = urlParams.get("lang");
    const langSet = Cookies.get("aliyun-lang");

    setLocale(langSet ?? urlLang ?? "zh-CN");

    let res = await checkConfigStatus();

    let inited = res.data?.inited ?? false;

    if (!inited) {
        history.push(configPath);
        return {inited: false};
    }

    return {inited: true};
}

export const layout: RunTimeLayoutConfig = ({initialState, setInitialState}) => {
    return {
        //logo: <img src={imgUrl} />,
        logo: <SvgMaxcompute/>,
        actionsRender: () => [<SelectLang key="SelectLang"/>],
        layout: "top",
        disableContentMargin: true,
        onPageChange: () => {
            const {location} = history;
            if (!initialState?.inited && location.pathname !== configPath) {
                history.push(configPath);
            }
        },
        breadcrumbRender: (routes: Route[]) => {
            let paths = decodeURI(location.pathname.substring(1)).split("/");
            if (paths.length > routes.length) {
                for (let i = routes.length, n = paths.length; i < n; i++) {
                    routes.push({
                        breadcrumbName: paths[i],
                        path: "/" + paths.slice(0, i + 1).join("/")
                    });
                }
            }

            return routes;
        },
    };
};

const MMAProBreadcrumb: React.FC<BreadcrumbProps> = (props) => {
    const value = useContext(RouteContext);

    return (
        <div
            style={{
                height: '100%',
                display: 'flex',
                alignItems: 'center',
            }}
        >
            <Breadcrumb {...value?.breadcrumb} {...value?.breadcrumbProps} {...props} />
        </div>
    );
};

