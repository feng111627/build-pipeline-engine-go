# 本地构建流水线执行引擎（Go）

一个零外部依赖的本地构建流水线引擎：从 JSON 配置读取构建步骤及其依赖关系，
在本地以受控并发执行，并提供**确定性的调度、缓存与失败处理**结论。

- 依赖图解析与校验（重复定义 / 悬空依赖 / 环形依赖明确报错，绝不死锁）
- 受 `concurrency` 上限约束的并发调度，无依赖关系的步骤并行执行
- 基于「命令 + 输入 + 依赖键」的稳定缓存键，命中即跳过，变化即失效，
  且缓存键与执行顺序、声明顺序无关
- 失败重试与失败传播：区分「自身失败 failed」和「被上游拖累 skipped」；
  与失败无关的分支继续执行
- 外部取消 / 全局超时停掉正在运行的步骤（连子进程一起杀），并清理半成品产物

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pipeline/types.go` | 配置 / 结果 / 状态类型，`Duration` 的 JSON 编解码 |
| `pipeline/errors.go` | 可区分原因的错误类型（`ConfigError` / `StepError` / `SkipError`） |
| `pipeline/graph.go` | 依赖图校验（重复 / 悬空 / 环）与确定性拓扑序 |
| `pipeline/cache.go` | 稳定缓存键计算、本地磁盘缓存（原子写） |
| `pipeline/runner.go` | 命令执行抽象、`sh -c` 本地执行器、测试用脚本执行器 |
| `pipeline/executor.go` | 单步骤执行：缓存查询、尝试超时、重试、产物清理 |
| `pipeline/scheduler.go` | DAG 并发调度、失败传播、取消 / 超时传播 |
| `pipeline/engine.go` | 配置加载、总装入口 |
| `cmd/pipeline/main.go` | 命令行：`run` / `validate` |

## 1. 配置格式

配置文件是 JSON（解析时 `DisallowUnknownFields`，拼错字段会直接报错）：

```json
{
  "concurrency": 3,
  "timeout": "5m",
  "steps": [
    {
      "name": "build",
      "command": "make build > build.log",
      "inputs": [
        { "name": "mode", "value": "release" },
        { "name": "config", "file": "config/app.yaml" }
      ],
      "outputs": ["build.log"],
      "depends": ["lint"],
      "retries": 2,
      "timeout": "2m"
    },
    { "name": "lint", "command": "make lint" }
  ]
}
```

### 顶层字段

| 字段 | 说明 |
| --- | --- |
| `concurrency` | 同时运行的步骤数上限，省略或 `<=0` 时默认为 `1` |
| `timeout` | **整条流水线**的全局截止时间（`"500ms"` / `"2m30s"` 等），到点取消全部运行中的步骤 |
| `steps` | 步骤数组 |

### 步骤字段

| 字段 | 说明 |
| --- | --- |
| `name` | 步骤名，唯一、非空 |
| `command` | 通过 `sh -c` 在本地执行的命令 |
| `inputs` | 输入列表，参与缓存键计算；元素二选一：`{"name","value"}` 字面值，或 `{"name","file"}` 文件（按内容 SHA-256） |
| `outputs` | 产物路径列表；步骤失败 / 被取消时这些路径会被删除，不留半成品 |
| `depends` | 直接依赖的步骤名列表；依赖全部成功（或缓存命中）后才会执行 |
| `retries` | 首次失败后的重试次数，总尝试次数 = `retries + 1` |
| `timeout` | **单次尝试**的超时；超时算一次失败尝试，仍计入重试 |

时长统一用 Go duration 字符串（`"300ms"`、`"5s"`、`"2m"`）。

### 配置错误（原因可区分，构建前拒绝）

| 错误 kind | 触发条件 | 报错示例 |
| --- | --- | --- |
| `duplicate_step` | 两个步骤同名 | `duplicate step definition: build` |
| `unknown_dependency` | 依赖指向不存在的步骤 | `dependency points to a step that does not exist: build -> ghost` |
| `cyclic_dependency` | 依赖成环（含自依赖） | `cyclic dependency detected: a -> b -> a` |
| `empty_step_name` | 步骤名为空 | `step name must not be empty` |

可以先静态校验，不会执行任何命令：

```sh
go run ./cmd/pipeline validate -config examples/pipeline.json
```

## 2. 缓存策略

每个步骤的缓存键是对以下规范化 JSON 取 SHA-256：

```json
{
  "command": "<命令原文>",
  "inputs":  [{"name":"...","value":"..."},
              {"name":"...","file":"...","content":"<文件SHA-256>"}],
  "deps":    [["<依赖名>","<依赖缓存键>"], ...]
}
```

要点：

1. **输入排序**：inputs 按 `name` 排序、deps 按依赖名排序后再哈希——
   所以换声明顺序、换 map 遍历顺序（执行顺序）**不会**改变缓存键。
2. **文件按内容**：`file` 输入哈希的是文件内容，时间戳 / 路径变化不影响，内容相同键相同。
3. **传递失效**：依赖键被纳入本步骤键。上游输入或命令变化后，上游及其所有
   下游的键都会级联变化，自动重跑，不需要手动 clean。
4. **命令变化**：`command` 变化立即失效。
5. 只有**执行成功**的步骤才写缓存（先写临时文件再 rename，原子落盘）；
   失败 / 取消不写，下次仍会重跑。
6. 缓存目录可重复使用：`-cache` 指定（默认 `.pipeline-cache`）。

缓存记录同时保存了每次尝试的命令和输出，可直接查看：

```sh
find .pipeline-cache -name '*.json' | head
```

## 3. 并发规则

- 一个协调 goroutine + 固定 `concurrency` 个 worker；协调者只在
  「某步骤完成」或「ctx 取消」上阻塞，因此**不可能死锁**。
- 就绪顺序确定：拓扑序中始终按名字最小者先派发（同层可并行，派发顺序确定）。
- 步骤在所有直接依赖达到 `success` / `cached` 后才入队。
- `concurrency=1` 即严格按拓扑序串行执行。

## 4. 失败 / 取消处理规则

### 步骤终态

| 状态 | 含义 |
| --- | --- |
| `success` | 执行成功 |
| `cached` | 缓存命中，未启动命令 |
| `failed` | 自己执行了且重试耗尽（错误类型 `*StepError`） |
| `skipped` | **没跑**，因为某条上游链 failed / canceled（错误类型 `*SkipError`，内含上游名和原因） |
| `canceled` | 正在运行（或全局取消时尚未运行）时被外部取消 / 全局超时打断 |

规则：

- 失败按 `retries` 重试；单步 `timeout` 算一次失败尝试，会继续重试。
- 重试耗尽后，全部**直接/间接后继**标记为 `skipped`，绝不运行；
  `SkipError.Upstream` 指出是被谁拖累。
- 与失败步骤无依赖关系的分支照常执行。
- 外部 `context.CancelFunc()` / 全局 `timeout` 到点：正在运行的步骤立即
  `canceled`（通过进程组 SIGKILL 杀掉 `sh` 及其所有子进程，不留僵尸），
  未开始的后继 `skipped/canceled`；这种情况**不重试**。
- 步骤失败或被取消时，其 `outputs` 声明的路径一律删除，不留半成品产物。
- 流水线退出码：全部 `success/cached` 为 `0`，否则 `1`；配置本身非法为 `2`。

## 5. 怎么验证

### 5.1 单元测试（按业务域组织，日志含业务 / 用例 / 结果）

```sh
go test ./... -race -count=1 -v
```

覆盖的业务域与关键边界：

- **依赖图解析与拓扑序**：确定性拓扑序、重复定义、悬空依赖、环形依赖
  （含自依赖）、空步骤名。
- **缓存键稳定性与失效**：稳定键、输入声明顺序无关、值变化失效、
  命令变化失效、依赖键传递失效、依赖顺序无关、文件内容变化失效、
  缺失文件报错；缓存落盘 / 读回。
- **调度与并发**：按依赖拓扑推进、实测在跑数不超过并发上限且能达到上限、
  缓存命中不启动命令、上游变更下游级联重跑。
- **失败重试**：重试后成功、重试耗尽 `failed` + `StepError`、失败清理产物。
- **失败传播**：后继全部 `skipped` 且错误可区分、无关分支继续、
  钻石依赖汇聚点跳过且能指出责任上游。
- **取消与超时**：外部取消杀掉运行步骤 / 清理产物 / 后继不跑、
  全局超时整线取消且调度器准时退出（无死锁）、单步超时按重试处理。
- **端到端真实命令**：用 `ShellRunner` 跑真实 `sh -c`，验证产物、
  二次运行全 cached、命令变更后级联重跑。

每条用例都通过 `t.Logf("结果: ...")` 输出结论，`-v` 日志里可以直接看到
覆盖了哪些业务、各自有哪些用例和结果。

### 5.2 手工冒烟（CLI）

```sh
# 静态校验
go run ./cmd/pipeline validate -config examples/pipeline.json

# 第一次：实际执行
go run ./cmd/pipeline run -config examples/pipeline.json -cache /tmp/pc

# 第二次：全部 cached
go run ./cmd/pipeline run -config examples/pipeline.json -cache /tmp/pc

# 修改某个 value 或文件输入后再跑：变化链路上的步骤自动重跑
# 全局超时演示（运行中的步骤会被 canceled）
go run ./cmd/pipeline run -config examples/pipeline.json -timeout 10ms
```

报告会按步骤打印状态、尝试次数、耗时、缓存命中与失败原因，末尾汇总
failed / skipped / canceled 数量。

## 6. 作为库使用

```go
cfg, err := pipeline.LoadConfig("pipeline.json") // *pipeline.Config
if err != nil { /* ... */ }

eng := &pipeline.Engine{} // 默认 ShellRunner；测试可注入 ScriptedRunner
report, err := eng.Run(context.Background(), cfg, ".pipeline-cache")
if err != nil { /* 配置错误，流水线未启动 */ }

for _, r := range report.Failed()  { /* r.Err 是 *StepError */ }
for _, r := range report.Skipped() { /* r.Err 是 *SkipError */ }
os.Exit(map[bool]int{true: 0, false: 1}[report.OK()])
```
