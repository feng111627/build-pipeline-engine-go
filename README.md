# build-pipeline-engine-go

本地构建流水线执行引擎：从本地 JSON 配置读取构建步骤及其依赖关系，在本地调度执行，
保证调度、缓存与失败处理的结论可预期、可复现。

## 快速开始

```bash
go build -o pipeline ./cmd/pipeline
./pipeline -config pipeline.json            # 运行流水线
./pipeline -config pipeline.json -timeout 5m  # 带整体超时
```

退出码：`0` 全部成功（或命中缓存）；`1` 有步骤失败或被取消；`2` 配置或依赖图错误。

## 配置格式

```json
{
  "concurrency": 4,                  // 并发上限，<=0 或缺省表示不限制
  "workdir": ".",                    // 步骤执行的工作目录（相对配置文件所在目录）
  "cachedir": ".pipeline-cache",     // 缓存目录（相对配置文件所在目录）
  "steps": [
    {
      "name": "build",               // 步骤名，全局唯一
      "command": ["go", "build", "./..."],  // 命令及参数（必填）
      "inputs": ["go.mod", "main.go"],      // 输入文件，参与缓存键计算
      "deps": ["prepare"],           // 依赖的步骤名（可选）
      "retries": 2                   // 失败后的重试次数，不含首次执行（可选，默认 0）
    }
  ]
}
```

`workdir` / `cachedir` 缺省分别为配置文件所在目录和该目录下的 `.pipeline-cache`。

## 依赖图与调度规则

- 启动前对依赖图做静态校验，三类错误**可区分**地报出并以退出码 2 终止，
  不会死锁、卡住或悄悄跳过：
  - `步骤重复定义`（`ErrDuplicateStep`）
  - `依赖指向不存在的步骤`（`ErrMissingDep`）
  - `检测到环形依赖`（`ErrCycle`，含自依赖，报错信息给出完整环路径）
- 调度按拓扑序进行：一个步骤的所有依赖完成后才会启动；
  互不依赖的步骤并发执行，但同时在跑的步骤数不超过 `concurrency`。
- 调度结论与配置中步骤的书写顺序无关（拓扑分层结果按字典序稳定）。

## 缓存策略（增量执行）

- 每个步骤的缓存键 = SHA-256（命令逐元素 + 按路径排序后的输入文件内容）。
- 稳定性：输入文件先排序再哈希，**与声明顺序、步骤执行顺序无关**；
  哈希覆盖文件内容而非修改时间，内容不变键就不变。
- 失效条件：命令变化、任一输入文件内容变化、输入文件出现/消失，都会改变缓存键。
- 命中缓存的步骤标记为 `CACHED` 并跳过执行（尝试次数为 0）。
- **只有完整成功的步骤才写入缓存记录**；失败或被取消的步骤不留下任何缓存，
  因此不会出现"半成品产物被当成有效缓存"的情况。

## 失败处理与取消

步骤最终状态（`Status`）：

| 状态 | 含义 |
|---|---|
| `SUCCESS` | 执行成功 |
| `CACHED` | 缓存命中，未执行 |
| `FAILED` | 自身执行失败，且重试（`retries` 次）已耗尽 |
| `SKIPPED_UPSTREAM` | 因上游失败/取消而未能执行（被拖累，自身没跑过） |
| `CANCELED` | 因外部取消/超时而中断 |

规则：

- 失败步骤按 `retries` 重试，全部耗尽后标记 `FAILED`。
- `FAILED` / `CANCELED` 步骤的所有后继标记 `SKIPPED_UPSTREAM` 且不会执行，
  与失败无关的分支继续正常执行。
- 外部取消（Ctrl-C）或 `-timeout` 超时：停止调度新步骤，
  正在运行的命令**整个进程组**被 SIGKILL 终止（包括 shell 派生的子进程），
  未开始的步骤标记 `CANCELED`；取消的步骤不写缓存，不留半成品。

## 测试与验证

```bash
go test ./... -race -v
```

测试按业务分组，日志中可见每组覆盖的用例与结果：

- `TestValidate` / `TestLayers` —— 依赖图校验（重复定义、缺失依赖、环形依赖、自依赖）与拓扑分层稳定性；
- `TestCacheKeyStability` / `TestCacheKeyInvalidation` / `TestCacheStore` —— 缓存键稳定性（顺序无关）与失效条件（内容/命令/文件集合变化）、缓存存取；
- `TestScheduleConcurrency` —— 并发上限约束、并行提速、依赖就绪顺序；
- `TestExecutorCache` —— 端到端缓存命中跳过、输入变化后失效重跑；
- `TestFailureRetryAndPropagation` —— 重试次数、失败后继跳过且无产物、无关分支继续、失败不写缓存；
- `TestCancellation` —— 超时取消及时生效、后继不执行无产物、取消不写缓存。

手动验证（端到端）：

```bash
./pipeline -config pipeline.json   # 第一次：全部 SUCCESS
./pipeline -config pipeline.json   # 第二次：全部 CACHED
# 修改任一 inputs 文件后再运行：对应步骤重新执行
```

## 项目结构

```
cmd/pipeline/main.go        # CLI 入口：加载配置、校验、执行、打印结果
internal/pipeline/config.go   # 配置结构定义与加载
internal/pipeline/graph.go    # 依赖图校验（三类可区分错误）与拓扑分层
internal/pipeline/cache.go    # 稳定缓存键计算与磁盘缓存存储
internal/pipeline/executor.go # 调度器：并发限制、重试、失败传播、取消
```
