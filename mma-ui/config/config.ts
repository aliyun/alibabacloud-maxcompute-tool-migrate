import { defineConfig } from "@umijs/max";

export default defineConfig({
    antd: {},
    model: {},
    initialState: {},
    request: {},
    layout: {
        title: 'MMA',
        locale: false,
    },
    routes: [
        {
            path: '/',
            component: './Home',
            hideInMenu: true,
            redirect: "/sources"
        },
        {
            icon: 'consoleSql',
            name: '数据源',
            path: '/sources',
            component: './Source/SourceList',
        },
        {
            name: "",
            path: '/sources/:name',
            component: './Source/SourceDetail',
            hideInMenu: true
        },
        {
            name: '',
            path: '/sources/:name/:dbName',
            component: './Source/SourceDb',
            hideInMenu: true
        },
        {
            name: '添加数据源',
            path: '/sourcesNew',
            component: './Source/NewSource',
            hideInMenu: true,
        },
        {
            name: ' 迁移任务',
            icon: 'CarryOut',
            path: '/jobs',
            routes: [
                {
                    name: "任务列表",
                    path: '/jobs/',
                    component: './Job/JobList',
                },
                {
                    name: "子任务列表",
                    path: '/jobs/tasks',
                    component: './Job/TaskList',
                }
            ]
        },
        {
            name: ' MMA配置',
            icon: 'setting',
            path: '/config',
            component: './Config',
        },
        {
            name: ' 帮助',
            icon: 'questionCircle',
            path: '/help',
            component: './Help',
        },
    ],
    npmClient: 'npm',
    proxy: {
        '/api': {
            'target': 'http://127.0.0.1:6060/api',
            'changeOrigin': true,
            'pathRewrite': { '^/api' : '' },
        },
    },
});

