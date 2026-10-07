# 多仓库存调拨

多个工厂共享关键零件时，缺料工厂会同时发起调拨。本项目交付一个 Python 后端，
维护**仓库批次、调拨申请、审批、装运、签收**，确认出库时在**单一数据库事务内**
把来源批次可用量原子转为在途并固化来源批次；部分装运、拒收、改道、超时与并发申请
通过**状态机 + 复式补偿分录**处理，每件数量在「出库 → 在途 → 入库」三个位置
都可追踪。

## 核心设计

### 原子出库，杜绝库存消失 / 重复发运

确认装运（`confirm_shipment`）在一个 `BEGIN IMMEDIATE` 事务内完成全部动作，
任一步骤失败整体回滚，不存在「先扣出库、再建在途」的中间窗口：

1. 条件更新原子扣减每个来源批次：
   `UPDATE batches SET available = available - ? WHERE id = ? AND available >= ?`
   —— 余量不足则该单整体失败，失败方重试读到的是已提交后的最新余量；
2. 在途量锚定到目的地仓的在途批次（每仓每零件一行 `__IN_TRANSIT__`）；
3. 固化来源批次明细 `shipment_lines`（每件数量从哪批出库）；
4. 写**平衡复式分录**（借在途 / 贷可用，按来源批次分行）；
5. 推进装运单与申请单状态机。

装运单「已计划」阶段不占库存，只有「确认」才扣减；重复确认被状态机幂等拒绝。

### 状态机

- 申请单：草拟 → 待审批 → 已批准 → 部分装运 → 已发运 →（部分签收）→ 已签收；
  驳回后可修改重提；含在途货物取消后由超时流程退回；已超时/退回可重新发运。
- 装运单：已计划 → 在途 → 部分签收 → 已签收；在途可改道（自循环，仅目的地变）；
  在途/部分签收可拒收剩余；逾期未签收判「已超时」。

合法迁移集中在 `src/transfer/enums.py` 的 `REQUEST_TRANSITIONS` /
`SHIPMENT_TRANSITIONS`，非法迁移抛 `InvalidTransitionError`。

### 补偿分录

- **拒收**：剩余在途原子退回各来源批次可用量，写「拒收退回」反向平衡凭证，
  申请单已发运量同步冲减，可重新补发。
- **超时**：`force_timeout_shipment` / `sweep_timeouts` 把逾期在途退回来源仓，
  写「超时退回」凭证；已部分签收的数量保留，只退回未签收部分；
  申请已取消则只冲数量、不重开申请。
- **改道**：在途在目的地仓锚点间平衡划转（借新仓在途 / 贷旧仓在途），
  来源批次与数量不变。

### 全链路追踪

`receipt_lines` 记录每次签收/拒收中「某来源批次的多少数量进入哪个入库批次」。
`trace_quantity(request_id 或 shipment_id)` 按来源批次给出每件数量的位置：

```
shipped_qty == received_qty + rejected_qty + in_transit_qty
```

`GET /audit` 与 `service.audit()` 复核：每张凭证借贷平衡、
每批次「期初 + 账本净额 == 当前库存」。

## 目录

- `domain/contract.json`：领域角色、状态、守恒约束与样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/transfer/`：调拨后端
  - `enums.py`：申请单/装运单状态机与分录类型；
  - `store.py`：SQLite 存储（单连接 + 锁串行写事务，文件模式 WAL）；
  - `ledger.py`：平衡复式分录与守恒审计；
  - `service.py`：`TransferService` 门面（批次/申请/审批/装运/签收/拒收/改道/超时/追踪）；
  - `models.py`：只读视图；`http_api.py`：零依赖 JSON HTTP 接口。
- `tools/check_contract.py`：契约摘要检查。
- `tools/demo.py`：端到端场景演示（并发竞争、部分装运、改道、拒收补偿）。
- `tests/`：契约测试、28 项服务回归（含多线程并发）、3 项 HTTP 端到端测试。

## 验证

```bash
python3 -m unittest discover -s tests -v          # 32 个测试
python3 -m compileall -q src tools tests          # 编译检查
python3 tools/check_contract.py domain/contract.json
python3 tools/demo.py                             # 场景演示
```

启动 HTTP 服务：

```bash
PYTHONPATH=src python3 -m transfer.http_api --db transfer.sqlite --port 8080
```

接口示例（见 `src/transfer/http_api.py` 顶部路由表）：

```bash
curl -XPOST localhost:8080/batches -d '{"warehouse":"WH_A","part_no":"P1","batch_no":"B1","opening_quantity":100"}'
curl -XPOST localhost:8080/requests -d '{"part_no":"P1","quantity":60,"from_warehouse":"WH_A","to_warehouse":"WH_B"}'
curl -XPOST localhost:8080/requests/1/submit
curl -XPOST localhost:8080/requests/1/approve
curl -XPOST localhost:8080/requests/1/shipments -d '{"lines":[["B1",60]]}'
curl -XPOST localhost:8080/shipments/1/confirm       # 原子 可用→在途
curl -XPOST localhost:8080/shipments/1/receive -d '{"items":[["B1",40]],"dest_batch_no":"R1"}'
curl -XPOST localhost:8080/shipments/1/reroute -d '{"new_destination":"WH_C"}'
curl -XPOST localhost:8080/shipments/1/reject  -d '{"items":[["B1",20]],"reason":"破损"}'
curl localhost:8080/requests/1/trace                 # 每件数量位置
curl localhost:8080/audit                            # 守恒审计
```
