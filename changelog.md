### v3.4.1(20260915)
#### bugFix:
1. 修复 TiDB parser 解析 `SHOW CREATE TABLE` 时，非主键列使用 `utf16` 字符集或相关排序规则导致 `Unknown character set`、表结构预检失败的问题；在包初始化阶段注册 parser 已有的 UTF-16 字符集元数据，字符比较仍由数据库执行（`mysql/charset.go`）
   - 补充混合字符集、字符集名称大小写、显式排序规则、主键识别、生成 DML 和未知字符集拒绝测试（`mysql/charset_test.go`）

### v3.4.0(20260826)
#### feature:
1. CLI 与 SDK 支持从单表 `UPDATE`/`DELETE` 的全限定 `schema.table` 推断目标库，省略 `--database`/`Config.Database` 时会统一使用并回填 SQL schema

#### bugFix:
1. UPDATE 执行模板按 SQL 词法边界提取并原样保留 `SET` 赋值文本，AST 继续负责语句与目标结构校验；避免 schema 或表名包含 `set` 时被截断，也避免 AST Restore 改写操作符和字符串字面量
2. 不再在 Writer 初始化阶段全局删除 SQL 中的分号，避免 `SET` 字符串字面量里的分号被移除

#### note:
1. `--database`/`Config.Database` 与 SQL schema 同时提供时必须逐字一致（包括大小写），不一致会在建立数据库连接前失败；已有依赖“配置库覆盖 SQL schema”的调用需要统一两处库名

### v3.3.0(20260603)
#### feature:
1. 新增 TiDB 专属清理策略 `--tidb-rowid`（SDK `WithTiDBRowID`）：按 TiDB 隐藏行句柄 `_tidb_rowid` 分块 DELETE，让**无主键/唯一键**的 TiDB 非聚簇（NONCLUSTERED）表也能高效分批清理（`mysql/tidb_rowid_strategy.go`）
   - 采用 seek 游标（`WHERE _tidb_rowid > cursor ORDER BY _tidb_rowid LIMIT n`）+ `_tidb_rowid IN (...)` 删除，对 `SHARD_ROW_ID_BITS`/`AUTO_RANDOM` 造成的稀疏 rowid 健壮；游标只在 DELETE 提交后前移，始终复用冻结后的 WHERE
   - 运行时通过 `information_schema.tables.TIDB_PK_TYPE` 校验适用性，CLUSTERED 表清晰报错（老版本回退到 `_tidb_rowid` 探测）
   - 仅 DELETE；显式开关，不改 TiDB 默认行为（默认仍走 RangeStrategy）；与覆盖快路径/分区并发/`--force-chunking-column` 互斥
   - 复用既有 NOW() 冻结、`--max-rows`/`--max-duration-ms` 护栏、`--rows-per-sec`/sleep/lag 限流、重试机制
2. `internal/retry` 错误分类补充 TiDB 写冲突可重试码：`9007`（write conflict）、`8022`（commit 可安全重试）、`8028`（info schema changed）

### v3.2.0(20260602)
#### feature:
1. NOW() 冻结：执行前将 SQL 中的 `NOW()`/`CURRENT_TIMESTAMP` 等时间函数固化为单一时刻，保证长任务跨 chunk 的时间边界一致；冻结时间来自 DB session，查询数据库当前时间或还原冻结 literal 失败时会直接终止任务，不再降级使用原始 WHERE（`mysql/freeze.go`）
2. OceanBase 错误分类 + 指数退避重试：区分可重试/不可重试错误，仅在事务尚未产生行变更时安全重试（`internal/retry`）
3. 解析器升级：由 soar+pingcap 迁移到 `tidb/parser`，并抽出 `ChunkStrategy` 策略接口，为多策略/并发预留扩展点
4. 覆盖索引两阶段快路径（仅 DELETE）：先按覆盖索引 SELECT 候选主键、再按主键 `IN` 删除，避免回表；新增 `--select-order-by`/`--select-index`/`--select-cursor`（`OBCoveringStrategy`）
5. EXPLAIN 预检：执行前预估影响行数，大表触发确认；新增 `--preflight-threshold`/`--yes`（`internal/preflight`）
6. dry-run：`--dry-run` 只打印样例 SELECT/DELETE，不实际执行
7. 全局行速率限流：`--rows-per-sec` 在令牌桶（`--sleep`）与从库延迟（`--max-lag`）之外，叠加一个按行的全局速率上限（`0`=不限，`task/rows_limiter.go`）
8. 执行上限统一生效：`--max-rows`/`--max-duration-ms` 现对 range/covering/partition 三种策略均生效，达到即干净停止（移除 P2 仅快路径可用的临时限制）
9. OceanBase 分区并行 DELETE：`--partition-concurrency` 自动发现表分区并以 ≤N 个 worker 并行删除，每个 worker 独占分区与游标、按 `PARTITION(...)` 限定范围；限速器全局共享、从库延迟暂停对所有 worker 一起生效（`OBPartitionStrategy`）
10. SDK 侧新增 `WithOBCovering`/`WithPreflight`/`WithDryRun`/`WithMaxRows`/`WithMaxDuration`/`WithRowsPerSec`/`WithPartitionConcurrency` 选项

#### optimization:
1. 分区路径 `--max-rows` 精确生效：提交前在锁内按剩余额度预留并裁剪批次，多 worker 合计不超过、零超删
2. 分区名作为标识符注入 SELECT/DELETE 时做反引号转义（防御性硬化）
3. 连接池上限随 `--partition-concurrency` 调整，始终保留 +2 连接余量（`max(10, 并发数+2)`），避免高并发抢连接
4. 补全 CLI/SDK 文档参数表与默认值，新增 8 工人并发竞态（`-race`）测试

#### bugFix:
1. 修复覆盖索引/分区 DELETE 路径每个 chunk 后误用「睡眠令牌桶」做行级限流：原 `Bucket.Wait(affected)` 把删除行数当毫秒数等待（桶为 1 token=1ms），导致 `--sleep 0 --rows-per-sec 0` 时仍每 1000 行 chunk 隐式 sleep ~1 秒，多 worker 共享桶更将整表吞吐压到 ~1000 行/秒。现移除该调用，行级限流统一交给 `--rows-per-sec`（`0`=不限速、跑满速），新增回归测试断言 sleep=0 时不产生行数级等待

### v3.1.0(20260415)
#### bugFix:
1. 修复 OceanBase 场景下仅靠兼容 DDL 无法识别全局唯一键的问题；现在会额外通过 `SHOW INDEX` 发现主键/唯一键候选
2. 修复 `forced_chunking_column` 在 OceanBase 下无法命中兼容 DDL 中缺失的真实唯一键的问题，如 `uk_order_code(order_code)`

#### optimization:
1. `forced_chunking_column` 输入会自动忽略逗号两侧空格，复合键配置更稳健

### v3.0.1(20250309)
#### feature:
1. 在 getInfoFromTable 中通过 `select version()` 识别数据源类型（MySQL/TiDB/OceanBase）
2. OceanBase 场景下在 SHOW CREATE TABLE 前执行 `SET SESSION _show_ddl_in_compat_mode = true`，获取 MySQL 兼容 DDL

#### optimization:
1. 获取表结构阶段改为固定连接流程（Conn），确保会话变量与 SHOW CREATE TABLE 在同一连接执行
2. writer_get_info_test 合并至 writer_test

---

### v3.0.0(20250308)
#### feature:
1. 新增使用文档入口：CLI 使用手册[`doc/design/cli-usage.md`](doc/design/cli-usage.md)、SDK 使用手册[`doc/design/sdk-usage.md`](doc/design/sdk-usage.md)
2. 模块路径统一为 `github.com/SisyphusSQ/go-oak-chunk/v3`，便于通过 go get 拉取与发布
3. 新增 SDK 模式：根包 `oak` 暴露 `Executor`，支持 `NewExecutor`、`Run`、`Stop`、`UpdateSleep`、`UpdateMaxLag`、`GetStatus`，便于以库形式嵌入业务
4. 新增 `RateLimiter` 组件，封装令牌桶及 Sleep/MaxLag/Correct/NoConsiderLag，支持运行时动态调整
5. 新增 `log.NewFromSugaredLogger`，支持 SDK 侧注入已有 `*zap.SugaredLogger`
6. 新增 `NewSlaveCheckerWithContext`、`CheckVersionContext`，启动链路支持 context 取消
7. 项目结构调整：CLI 入口移至 `cmd/go-oak-chunk/main.go`，根包名改为 `oak`

#### optimization:
1. 全局 import 与 go.mod 模块路径更新为 `github.com/SisyphusSQ/go-oak-chunk/v3`，SDK 使用手册同步更新导入说明
2. 全面去除 `os.Exit`/`log.Fatal`，改为返回 `error`，支持 SDK 优雅失败处理
3. 日志从 tinylog 迁移至 zap + lumberjack，合并 `StreamLogger`/`GlobalLogger` 为单一 `Logger`
4. DB 调用统一改为 `ExecContext`/`QueryContext`，取消信号可及时中断阻塞 I/O
5. `Procedure`/`SlaveChecker`/`Writer` 中查询路径补齐 `rows.Close`、`rows.Err` 收尾
6. Stop/Cancel 统一映射为 `task.ErrExecutionStopped`，CLI 层按正常停止处理
7. `Executor` 采用 one-shot 语义，二次 `Run` 返回 `ErrExecutorAlreadyRun`
8. `RateLimiter` 对 sleep/maxLag/correct 负值做 clamp 保护
9. CLI 日志初始化收敛为配置解析后一次性初始化
10. 完善 `run` 命令错误与流程日志：配置加载/预检/Executor 创建/任务执行/pprof 失败时补充上下文参数，新增任务启动与完成 Info 日志，logger 未初始化时错误输出到 stderr

#### bugFix:
1. 修复 `Writer` 重试分支事务未回滚导致连接泄漏，失败时显式 `Rollback` 后再重试
2. 修复 `ProgressCallback` panic/阻塞导致 `Execute` 退出卡死，增加 recover 与非阻塞 done 超时
3. 修复 `Writer` 的 IsFinished/RowAffects/CostTime 数据竞态，改为 atomic 类型
4. 修复 `getStopTime` 不响应 context 取消，补充 `ctx` 参数并检测 `ctx.Done()`
5. 修复 cleanup 中 channel 重复关闭 panic，使用 `sync.Once` 保护
6. 修复 `conf.NewConfig` 中 `os.Open` 失败时仍执行 `defer file.Close` 的潜在问题
7. 修复 `mysql/writer` 的 `getInfoFromTable` 未检查 `rows.Err()`
8. 修复 `procedure`、`slave_checker` 多处 `rows` 未关闭或未检查 `rows.Err()`

### v2.1.0(20251207)
#### feature:
1. 支持TiDB和OceanBase的分chunk DML功能，使用时添加参数`--no-slaves`
