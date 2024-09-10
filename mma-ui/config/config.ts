import { defineConfig } from "@umijs/max";
import routes from "./routes";
import defaultSettings from './defaultSettings';

export default defineConfig({
    antd: {},
    model: {},
    initialState: {},
    request: {},
    layout: {
        title: 'MMA',
        locale: true,
        ...defaultSettings
    },
    /**
     * @name 开启 hash 模式
     * @description 让 build 之后的产物包含 hash 后缀。通常用于增量发布和避免浏览器加载缓存。
     * @doc https://umijs.org/docs/api/config#hash
     */
    hash: true,
    /**
     * @name 路由的配置，不在路由中引入的文件不会编译
     * @description 只支持 path，component，routes，redirect，wrappers，title 的配置
     * @doc https://umijs.org/docs/guides/routes
     */
    routes: routes,
    npmClient: 'npm',
    proxy: {
        '/api': {
            'target': 'http://127.0.0.1:6060/api',
            'changeOrigin': true,
            'pathRewrite': { '^/api' : '' },
        },
    },
    /**
     * @name 国际化插件
     * @doc https://umijs.org/docs/max/i18n
     */
    locale: {
        // default zh-CN
        default: 'zh-CN',
        antd: true,
        // default true, when it is true, will use `navigator.language` overwrite default
        baseNavigator: true,
    },
});

