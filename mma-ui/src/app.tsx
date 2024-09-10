import { AliyunOutlined } from '@ant-design/icons';
import { ProBreadcrumb, RouteContext} from "@ant-design/pro-components";
import {Route} from "antd/lib/breadcrumb/Breadcrumb";
import React, {useContext} from "react";
import {Breadcrumb, BreadcrumbProps} from "antd";
import {checkConfigStatus} from "@/services/config"
import { history } from '@umijs/max';
import type { RunTimeLayoutConfig } from '@umijs/max';
import {SelectLang} from "@/SelectLang";
import {setLocale} from "umi";

const configPath = '/config';

// 运行时配置

// 全局初始化数据配置，用于 Layout 用户信息和权限初始化
// 更多信息见文档：https://next.umijs.org/docs/api/runtime-config#getinitialstate
export async function getInitialState(): Promise<{ inited: boolean}> {
    const search = window.location.search;
    const urlParams = new URLSearchParams(search);
    setLocale(urlParams.get("lang") ?? "zh-CN");

    let res = await checkConfigStatus();

    let inited = res.data?.inited ?? false;

    if (!inited) {
        history.push(configPath);
        return {inited: false};
    }

    return {inited: true};
}

export const layout: RunTimeLayoutConfig = ({ initialState, setInitialState }) => {
    return {
        logo:  <AliyunOutlined/>,
        actionsRender: () => [<SelectLang key="SelectLang" />],
        layout: "top",
        disableContentMargin: true,
        onPageChange: () => {
            const { location } = history;
            if (!initialState?.inited && location.pathname !== configPath) {
                history.push(configPath);
            }
        },
        breadcrumbRender: (routes: Route[]) => {
            let paths = decodeURI(location.pathname.substring(1)).split("/");
            if (paths.length > routes.length) {
                for (let i = routes.length, n = paths.length; i < n; i ++) {
                    routes.push({
                        breadcrumbName: paths[i],
                        path: "/" + paths.slice(0, i+1).join("/")
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

