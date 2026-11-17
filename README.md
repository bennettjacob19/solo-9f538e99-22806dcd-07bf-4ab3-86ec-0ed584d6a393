# 告警值班控制台（FastAPI + SQLite + Docker Compose）

值班工程师维护服务时暂停新故障通知，同时保证通知端不会留下"已恢复但没通知"的故障。
控制台还内置单个监控探针的心跳监测：探针失联本身会产生一轮告警，避免把"没有新故障"误当作"服务正常"。
系统由两个容器组成：

| 服务 | 端口 | 职责 |
| --- | --- | --- |
| `alert-app` | 8000 | 告警控制台：事件接入、静默管理、告警/通知状态查询、发件箱 worker |
| `notifier` | 8001 | 本地通知端：接收通知、模拟丢回执、按通知号去重、展示最终状态 |

## 快速启动

```bash
docker compose up --build
# 控制台:   http://localhost:8000/
# 通知端:   http://localhost:8001/
```

`LOSE_FIRST_N=1` 表示每个**新通知号**的首次投递返回 HTTP 500（回执丢失，消息已落库），
应用随后用**同一个通知号**重试，通知端去重后返回成功。设为 0 可关闭模拟。

无 Docker/PyPI 的环境可用标准库 runner 启动（业务逻辑与 FastAPI 版本完全共用 `common/`）：

```bash
ALERT_PORT=8000 NOTIFIER_PORT=8001 \
NOTIFIER_URL=http://127.0.0.1:8001 \
ALERT_DB_PATH=./data/alerts.db NOTIFIER_DB_PATH=./data/notifier.db \
python3 -I runner/__main__.py
```

## 事件接入 API

`POST /api/events`

```json
{ "fingerprint": "hostA/disk", "seq": 1, "kind": "firing", "summary": "disk 92%" }
```

- `kind`：`firing`（触发）或 `recovery`（恢复）
- `(fingerprint, seq)` 唯一；`seq` 在同指纹内严格递增

### 幂等与顺序规则

| 场景 | 行为 |
| --- | --- |
| 同指纹同序同文重试 | 返回 `duplicate_replayed`，沿用原结果，不产生新通知 |
| 同指纹同序异文 | HTTP 409 `conflicting_event`，拒绝 |
| 旧序号（`seq <= current_seq`） | HTTP 409 `stale_event`，当前状态不被改写 |
| 无告警时的恢复 | HTTP 409 `stale_event` |
| 恢复后再次触发 | 轮次（round）+1，视为新一轮，重新走触发/恢复通知 |

## 探针心跳监测（单探针）

网页顶部「探针心跳监测」卡片设置超时秒数并实时展示状态；探针通过
`POST /api/probe/heartbeat` 上报心跳（无需 body），**期限一律按服务端接收时间**
（最近心跳 + 超时秒数）计算，与探针自身时钟无关。

| 状态 | 含义 |
| --- | --- |
| 待接入 `pending` | 已设置阈值（或从未配置），尚未收到首次心跳；**此阶段不报失联** |
| 在线 `online` | 心跳在期限内 |
| 失联 `lost` | 超过期限未收到心跳 |

规则：

- 到期只产生**一轮**失联告警（轮次 +1），持续缺席不重复；恢复心跳后自动恢复，
  再次超时才会开启新一轮。
- 失联/恢复以保留指纹 `probe:heartbeat` 走与普通事件**完全相同的告警流水线**
  （静默暂停新触发但不拦截已通知故障的恢复、发件箱按指纹严格保序、丢回执同号重试），
  因此恢复通知绝不会抢在触发之前送达。
- 普通事件禁止使用 `probe:` 前缀的指纹（HTTP 409 `reserved_fingerprint`），
  失联告警与普通事件互不覆盖。
- 心跳在「已超时、巡检尚未写入失联」的窗口到达时，会先记录这一轮失联（含触发通知）
  再立即恢复，失联不会被悄悄吞掉；心跳与巡检在同一事务锁下串行，并发不会产生两轮。
- 探针状态（最近心跳、期限、轮次）持久化在 SQLite，**进程重启后期限继续有效**，
  重启后超时未心跳仍会转为失联。
- 未设置阈值时若心跳先到，按 `PROBE_DEFAULT_TIMEOUT_SECONDS`（默认 60s）自动建档。

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/probe` | 探针状态（状态、阈值、最近心跳、期限、剩余秒数、失联轮次） |
| `POST /api/probe/config` | 设置超时秒数：`{"timeout_seconds": 30}`（按最近心跳重算期限） |
| `POST /api/probe/heartbeat` | 上报一次心跳（期限按服务端接收时间计算） |

## 静默规则

- `POST /api/silence`，body：`{"duration_seconds": 60, "reason": "维护"}`
  （也支持绝对时间 `until_ts`）；`DELETE /api/silence` 立即解除；`GET /api/silence` 查询。
- 静默期内的新触发**只更新告警**（序号/摘要），不产生通知。
- **此前已通知故障的恢复不受静默影响**，恢复通知照常入发件箱。
- 静默到期（或手动解除）后，worker 只补发**仍在触发且本轮从未通知**的故障；
  静默期内已经恢复的轮次保持静默，不会补发。

## 通知投递与去重

- 通知号格式：`{fingerprint}#r{round}:{kind}`，同一轮的 firing/recovery 各一个稳定号码，
  重试永远复用该号码。
- 发件箱表（`notifications`）状态机：`pending → in_transit → delivered`，
  失败按 `attempts` 退避重试（1s 起，上限 30s）。
- **同指纹严格按状态顺序送达**：前一条未 delivered，后一条不会发出。
- 通知端以通知号为主键去重：首次见到的号码落库；同号重试返回 `duplicate: true`，
  只累加 `attempts_seen`；同号异文返回 409。
- 两个 SQLite 库都挂载在 Docker volume，重启后发件箱继续投递、通知端记录保留，
  不漏发、不重复；迟到的旧序号事件无法覆盖恢复后的状态。

## 其他 API

| 方法/路径 | 说明 |
| --- | --- |
| `GET /api/alerts` | 告警列表（含当前轮 firing/recovery 通知状态）+ 当前静默状态 |
| `GET /api/alerts/{fingerprint}/events` | 单指纹事件历史 |
| `GET /api/notifications` | 发件箱明细（状态、尝试次数、错误、入队/送达时间） |
| `GET /api/notifications`（通知端 8001） | 通知端收到的最终状态 |
| `GET /health` | 健康检查 |

## 目录结构

```
common/        # 框架无关的核心逻辑（事件、静默、发件箱、通知端去重）+ SQLite schema
app/           # FastAPI 告警控制台（后台 worker 线程）
notifier/      # FastAPI 本地通知端（丢回执模拟）
runner/        # 纯标准库 HTTP runner，复用 common，供无 PyPI 环境运行与自测
web/           # 两个网页（原生 HTML/CSS/JS，2s 自动刷新）
tests/e2e.py   # 端到端关键链路自测（真实 HTTP + SQLite，无第三方依赖）
Dockerfile / Dockerfile.notifier / docker-compose.yml
```

## 自测

```bash
python3 -I tests/e2e.py
```

覆盖：健康检查与页面/静态资源、幂等/冲突/旧序拒绝、触发恢复全链路、丢回执同号重试、
静默期抑制触发但恢复照发、静默到期仅补发仍触发告警、恢复后再触发新一轮、
同指纹顺序投递、进程重启持久化不重不漏、非法载荷校验；
探针心跳：首次心跳前不报失联、到期仅一轮失联、恢复按序送达、保留指纹拒绝普通事件、
静默期失联触发被抑制且解除后补发、重启后期限继续有效、迟到心跳先记录失联再恢复、
巡检与心跳并发不产生重复轮次。
