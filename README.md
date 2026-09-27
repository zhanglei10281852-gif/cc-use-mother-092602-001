# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

针对卫星计算载荷只能在有限散热窗口内运行的场景，服务提供独立的热窗口调度领域：值班工程师配置带功率与能量预算的散热窗口，提交带预计耗时、功耗和优先级的计算请求，工作者在窗口开放时领取执行；窗口即将结束或租约过期时任务被安全暂停并完整记录，后续窗口按原始优先级和剩余工作量恢复执行，完成后生成不可变回执，全流程可审计。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 排队领取：按优先级和进入队列的顺序分配任务，工作者可声明算法能力并获得有期限的租约。
- 执行回执：工作者可以续租、提交结果或报告失败；可重试错误使用确定的退避时间重新排队。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

## 卫星载荷热窗口调度（/api/payload）

- 热窗口配置：`POST /api/payload/windows` 保存窗口起止时间、并发功率上限（毫瓦精度）、并发任务数、能量预算和安全暂停余量；`window_key` 幂等，参数冲突返回 409。
- 任务提交：`POST /api/payload/tasks` 接收预计耗时、功耗、优先级和业务负载；同一提交人同一 `request_id` 重复提交返回原任务，指纹不同返回 409。
- 排队与领取：`POST /api/payload/tasks/claim` 只在窗口开放时发任务，按优先级与提交顺序选择，并校验并发数、功率余量和能量预算；响应携带 `must_pause_by`（窗口结束减余量、租约、预算耗尽三者取最早）。
- 心跳与进度：`POST /api/payload/tasks/{id}/heartbeat` 上报单调递增的已执行秒数并续租，响应告知 `should_pause`、`ready_to_complete` 与窗口剩余时间。
- 安全暂停：`POST /api/payload/tasks/{id}/pause`、窗口关闭 `POST /api/payload/windows/{id}/close` 和过期清扫 `POST /api/payload/windows/sweep` 都会把运行中任务转为已暂停，写入执行段（上报秒数、入账秒数、能量、结束原因）和审计事件，任务不会无记录丢失。
- 跨窗口恢复：已暂停任务保留原始优先级与剩余工作量，在新窗口开放时被领取（审计记录 `task.resumed`），也可通过 `POST /api/payload/tasks/{id}/resume` 人工重新排队。
- 完成回执：`POST /api/payload/tasks/{id}/complete` 汇总所有执行段生成不可变回执（总耗时、总能量、内容摘要），`GET /api/payload/tasks/{id}/receipt` 可查询。
- 审计查询：`GET /api/payload/audit` 按任务、窗口、动作、操作者和时间范围过滤；`GET /api/payload/windows/{id}/energy` 给出窗口能量核算。
- 可复现排期：`POST /api/payload/plan` 与 `python -m app.cli payload-plan` 共用同一个纯函数仿真器，内部全部使用整数秒、毫瓦、毫焦计算，相同输入在 API 与命令行下产生完全一致的结果。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复，并保留身份与既有科学计算模块的回归用例。热窗口调度另有专项用例：窗口幂等配置、请求指纹冲突、功率与能量预算约束、心跳进度、安全暂停与执行段核算、跨窗口恢复保持优先级与剩余工作量、完成回执摘要、过期窗口与租约清扫，以及 API 与命令行排期结果的一致性。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
python -m app.cli payload-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。`payload-demo` 演示热窗口完整链路：配置窗口、幂等提交、领取、心跳、安全暂停、关闭窗口、跨窗口恢复和完成回执，并输出逐项检查结果。

## 可复现排期仿真

```bash
python -m app.cli payload-plan --spec '{"windows":[...],"tasks":[...],"now":"2026-09-26T00:00:00+00:00"}'
python -m app.cli payload-plan --input plan.json
```

排期规格包含窗口（起止、功率上限、并发数、能量预算、安全余量）和任务（优先级、剩余秒数、功耗）。输出每个执行段的起止、能量与结束原因、每个任务的完成时间与剩余工作量、每个窗口的利用率与总量汇总，与 `POST /api/payload/plan` 的结果逐字段一致。

## 目录结构

```text
app/
  compute/         计算模板、配额、任务、结果版本和人工干预
  payload/         卫星载荷热窗口调度、暂停恢复、回执、审计与排期仿真
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查、冒烟和排期仿真入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营、热窗口调度和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
