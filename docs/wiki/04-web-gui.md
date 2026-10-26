# Web GUI 使用说明

> 管理界面在 `web/`，通过管理 API（默认 `:8080`）读取 Broker 状态并执行操作。集群相关页面属于 v0.7.0，尚未实现。

## 架构

| 组件 | 技术 |
|------|------|
| 框架 | React 19 + Vite |
| UI 库 | shadcn/ui + Tailwind CSS 4 |
| 路由 | TanStack Router |
| 图表 | Recharts |
| 状态管理 | Zustand |
| 国际化 | i18next（zh-CN / en-US） |

## 启动

```bash
cd web
npm install
npm run dev
```

浏览器打开 Vite 打印的地址（默认 `5173`）。需要 Node.js 20+。

## 连接 Broker

- 开发服务器把 `/api` 代理到 `127.0.0.1:8080`。Broker 部署在其他机器时，在「设置」里改管理 API 地址。
- `api.token`（或 `HERON_API_TOKEN`）配置了鉴权时，在「设置」里填写同一个值，浏览器会以 `?token=` 携带给 WebSocket（升级请求无法设置 `Authorization` 头）。
- 界面语言（简体中文 / English）与明暗主题在「设置」里切换。

## 页面

| 页面 | 功能 |
|------|------|
| 仪表盘 | 连接数、主题数、订阅数、消息速率（计数器差分），最近一分钟的吞吐与连接趋势图 |
| 客户端 | 按 ID / 在线状态 / 协议版本筛选，查看会话字段与订阅，断开连接 |
| 主题 | 树形列表、保留消息查看，通过管理 API 发布或清除保留消息 |
| 订阅 | 全部订阅关系列表 |
| 规则 | 规则的增删改查；Broker 按规则执行消息路由，规则重启后恢复 |
| 设置 | 管理 API 地址、token、界面语言、主题 |

## 数据推送

`GET /api/v1/events` WebSocket 每秒推送一次 `type: snapshot`（计数与速率）；基线之后的连接/断开以 `type: client` 事件推送。

## 相关代码

- `web/src/` 源码；`npm run lint`、`npm run typecheck`、`npm test`
- 组件按 shadcn 惯例添加：`npx shadcn@latest add <component>`
