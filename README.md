# 多仓库存调拨

本项目维护多仓库存调拨的领域约定、角色边界与样例数据，并提供可运行的 Python 后端服务，
覆盖仓库批次、调拨申请、审批、装运、签收全流程，供接口和自动化验证统一使用。

## 后端服务（src/transfer_backend/）

多个工厂共享关键零件时，缺料工厂会同时发起调拨。本服务的核心保证：

- **确认即原子**：装运确认在**单个数据库事务**内完成「扣减来源批次预留/在库 → 核销预留明细 →
  写在途记录（携带来源批次）→ 写台账分录 → 推进状态机」。不存在"先扣出库再补在途"的中间态，
  任何一步失败整体回滚，失败重试不会丢库存或重复发运。
- **重试安全**：所有写接口接受 `Idempotency-Key` 请求头，幂等记录与业务写入**同事务提交**；
  装运确认同时具备天然幂等（已在途的单据重复确认直接返回当前状态）。
- **并发不超卖**：库存扣减一律使用「条件更新 + 影响行数校验」的原子 SQL（compare-and-swap），
  状态迁移使用守卫更新（`UPDATE ... WHERE status = ...`）兼作行锁。
- **全程可追踪**：双式台账记录每一次跨桶移动；追踪接口可回答"批准量里每一件当前处于
  预留 / 在途 / 已签收 / 已退回 / 已取消 哪个位置"。

### 领域不变量与实现对应

| 契约不变量 | 实现 |
| --- | --- |
| 跨仓批次守恒 | `ledger.py` 双式分录（每事务组借贷平衡）+ `GET /parts/{id}/reconciliation` 台账与计数器对账 |
| 在途状态转换 | `enums.py` 状态机白名单 + `services.py` 守卫更新，非法迁移返回 409 |
| 部分装运签收 | 一次申请可多次装运、一次装运可多次签收，`ShipmentLine` 逐行条件核销 |
| 失败补偿分录 | 拒收 `REJECT_RETURN`、超时退回 `TIMEOUT_RETURN`、取消 `RELEASE`，均携带 `compensates_txn` 指向原事务组 |

### 状态机

调拨申请：`DRAFT → SUBMITTED → APPROVED → PARTIALLY_SHIPPED → SHIPPED → PARTIALLY_RECEIVED → RECEIVED`，
异常终态 `REJECTED / CLOSED / CANCELLED`（履约态由计数器推导，命令态显式迁移）。

装运单：`PENDING → IN_TRANSIT → PARTIALLY_RECEIVED → RECEIVED`，
异常分支 `TIMED_OUT`（超时扫描，可延期/签收/退回）、`REJECTED / RESOLVED_WITH_REJECTION / RETURNED / CANCELLED`。

### 主要接口（/api/v1）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/warehouses` `/parts` `/batches` | 档案与入库建档（台账记 INBOUND） |
| POST | `/transfer-requests` | 创建调拨申请（幂等键去重） |
| POST | `/transfer-requests/{id}/submit` `/approve` `/cancel` | 提交 / 审批（原子预留，不足整体回滚）/ 取消（释放预留） |
| POST | `/transfer-requests/{id}/shipments` | 建装运单（显式批次行或 FIFO 自动分配） |
| POST | `/shipments/{id}/confirm` | **原子确认**：可用/预留转在途并记录来源批次 |
| POST | `/shipments/{id}/receipts` | 签收（部分签收、逐行拒收，拒收写补偿分录） |
| POST | `/shipments/{id}/reroute` `/extend` `/return` | 在途改道 / 超时延期 / 退回发货仓 |
| POST | `/shipments/timeout-sweep` | 超时扫描，标记 TIMED_OUT |
| GET | `/transfer-requests/{id}/trace` | 逐数量位置追踪 + 守恒校验 |
| GET | `/parts/{id}/reconciliation` | 台账 vs 计数器对账 |
| GET | `/ledger` `/batches/{id}/ledger` `.../events` | 台账与状态机事件审计 |

## 运行

```bash
pip install -e .            # 或：pip install fastapi "uvicorn[standard]" sqlalchemy
python3 tools/run_server.py # 默认 127.0.0.1:8000，SQLite 落盘 ./transfer.sqlite3
```

数据库连接用环境变量覆盖（PostgreSQL 同样适用，并发语义不变）：

```bash
TRANSFER_DATABASE_URL=postgresql+psycopg://user:pass@host/db python3 tools/run_server.py
```

## 验证

测试命令（契约回归 + 后端共 17 项，含并发与崩溃注入用例）：

```bash
python3 -m unittest discover -s tests -v
```

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`

并发用例直接验证本系统的核心承诺：10 线程争抢 100 件库存恰好 3 单成功不超卖；
5 线程同时确认同一装运单库存只扣一次；在「扣库存」与「写在途」之间注入崩溃，
整体回滚且重试后只生效一次。

## 目录

- `domain/contract.json`：领域角色、状态、约束、样例与后端实现映射。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/transfer_backend/`：后端服务（FastAPI + SQLAlchemy）。
- `tools/check_contract.py`：命令行摘要检查；`tools/run_server.py`：服务启动。
- `tests/`：契约完整性与后端行为回归测试。
