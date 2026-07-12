# AGENTS.md

本文件是 data0 中 AI agent 与贡献者的工作约定。语义承诺面以 [README.md](./README.md) 为准；代码、测试与文档发生冲突时，先确认可执行行为，再修改约定。

## 代码 review 的证据纪律

### 1. 真实复现优先

- 报告“已确认缺陷”前，必须在当前 checkout 上得到确定、可重复的可执行复现。
- 复现必须记录：触发条件、执行命令或测试、预期结果、实际结果，以及运行的是源码还是生产构建。**必须核对实际错误的类型与消息原文**，不得凭输出形状（如"很深的栈"）推断错误类别——历史上曾把断言错误误报为栈溢出。
- 现有测试通过、覆盖率高、代码看起来可疑，都不能单独证明缺陷存在。
- 修复前先让最小复现失败；修复后用同一复现证明通过。
- **负面断言纪律**：“X 没有问题”类结论必须写成“在方法 M 下未发现 X 的反例”，注明方法与其可达范围。review 只能给出下界，任何方法都无法证明缺陷不存在。

### 2. 严格区分证据等级

Review 结论必须分为三类，不得混写：

1. **动态复现**：实际执行后观察到错误结果、异常、泄漏或状态破坏。
2. **静态确认**：无需运行即可验证的仓库事实，例如缺少 CI、锁文件与 manifest 不一致；必须明确标注“未运行触发”。
3. **待验证假设**：只有可疑路径或触发条件尚未闭合；不得写成已确认问题，也不得给出确定影响。

如果一个问题包含多个影响声明，每个声明都要分别说明证据等级。例如“下标写错”可以静态确认，但“因此泄漏 effect”仍需生命周期复现。

### 3. 复现必须安全、确定且可转成回归测试

- 优先使用最小 Vitest 用例；集合/增量算法优先与原生数据结构或强制全量重算做差分验证。
- 随机测试必须固定 seed，并在失败信息中输出 seed、步骤和操作。
- 当前已知缺陷可以临时使用 `test.fails` 保存可执行证据；修复时必须将其改为普通测试，禁止永久跳过。
- 测试必须清理创建的 effect、timer、临时目录，并恢复 Notifier 等全局单例状态，不能污染其他用例。
- 安全问题必须在临时目录、假命令或 mock 环境中复现。禁止为证明问题而执行真实 publish、push、删库、外部请求或其他破坏性操作。

### 3.1 修复必须向上游还债（等价类规则）

每个缺陷修复除实例级回归测试外，必须回答并落实两件事：

1. **等价类是什么**：这个缺陷所属的输入/状态/调度类别（例如"重复原始值下按值定位"而不是"push(0) 顺序错"）。
2. **哪层防线补上了这个类**：类型/构造性原语、dev 不变量断言、差分/性质测试、交错枚举，至少一层要能覆盖整个等价类，并作为常驻资产（而非一次性验证）进入仓库。

只修被复现的实例、只钉住该实例的回归测试，视为未完成的修复——历史上"替换型 splice 的重复值错位"修复未覆盖"纯插入/set 的重复值错位"，缺陷类因此存活了整整一轮 review。

### 3.2 深度 review 按方法立项

- 每轮深度 review 开始前必须声明：本轮引入哪种**此前未使用过**的方法（差分性质、对抗值域、调度交错枚举、mutation 审计、模型比对……）。
- 同一方法的重复审查只做回归确认，不计为新一轮深度 review。同方法多轮的边际产出趋近于零；发现新缺陷依赖方法类别的更换，而不是轮数的堆积。
- 已使用过的方法清单（新方法用毕后追加到此）：
  1. 通读源码 + 手写示例测试（早期各轮）；
  2. splice 参数域对抗（负数/越界/小数/NaN）与归一化横扫；
  3. 固定 seed 差分 fuzz（增量结果 ≡ 全量重算，含重复值域）；
  4. async 敌意调度与完成序交错枚举；
  5. 异常注入（订阅者/回调抛错后全局状态复原检查）；
  6. 生命周期审计（destroy/孤儿 effect/泄漏计数）；
  7. batch/延迟调度下的多 info 单次 digest 重放差分（既有差分 fuzz 全部在 batch 外逐操作断言，隐含"每次 digest 恰一条 info"的假设；重放语义本身是独立攻击面，尤其是 EXPLICIT_KEY_CHANGE 与结构操作混排）；
  8. destroy 僵尸行为横扫 + destroy 事件对称性检查（直接断言"destroy 后不再接收更新"；不依赖 retainedDiagnostics——它只统计 active=true 的 effect，源模式结构在其中完全不可见）。
  9. 生产构建（`__DEV__:false`）契约差分 + 对抗探针（object-atom 浅写、indexKeyDeps 惰性清扫、generator 异步重入、稀疏 set × map(index)、API/文档漂移）；资产：`__tests__/verifiedReviewFixes.spec.ts`（实例回归）、`__tests__/sparseSetOperatorsSweep.spec.ts`（"OOB set × 全派生算子族不崩溃且可恢复"的等价类横扫，含 batch 多 info 回退路径）。
  10. 既有攻击轴 × 未覆盖算子族的组合横扫（重复值域 × selection 家族、undefined 合法元素 × toSorted 的 EXPLICIT_KEY_CHANGE 路径、链式深层管道差分、回调重入）；发现并修复 createSelection 重复 item indicator 漂移（`itemToIndicators` 改 Set 广播 + 按身份精确移除）与 toSorted 变更含 undefined 时与全量 sort 分叉（回退全量重算）两个缺陷类；资产：`__tests__/deepReview2026H2Findings.spec.ts`（实例回归 + 两个等价类的常驻差分 sweep：重复 item 域 × selection 的"indicator ≡ currentValues 成员"不变量、undefined 值域 × toSorted 的"增量 ≡ 全量 sort"不变量，双 compare 形态）。既有 fuzz 的盲区模式：差分 fuzz 只喂"单一算子 × 唯一/重复数值域"，selection（值→indicator 的 Map 记账）与"undefined 作为合法元素值"两个维度都不在任何生成器的值域里。
  11. 模型比对（系统级管道网 ≡ 朴素参考模型）：多源 × 多派生 × 链式管道 × selection × RxSet/RxMap 派生组成一张网，随机操作序列（约 1/3 打包进 batch 形成多 info 重放）后逐节点与朴素 JS 全量重算比对；同轮引入生产语义 CI 差分（`pnpm test:prod`，`__DEV__:false` 编译源码跑同套测试，dev 特化文件排除清单见 `vitest.prod.config.ts`）。首跑均未发现反例。资产：`__tests__/modelComparisonFuzz.spec.ts`（同时是 RxSet 运算族/RxMap 派生/createSelection 的 batchReplay 列与链式管道的对账资产）、`vitest.prod.config.ts` + CI 步骤。

### 3.3 覆盖清单纪律（盲格必须可见）

2026-H2 教训：两个存活多轮的缺陷类都落在"攻击轴已存在、但没和该算子/值域相乘"的盲格上（重复值域 × selection、undefined 值域 × toSorted）。方法轮换有清单，覆盖矩阵却没有清单，盲格因此不可见。规则：

- `__tests__/coverageInventory.spec.ts` 是「公开派生算子 × 对抗维度 → 防线资产」的常驻账本，机械强制：引用的资产文件必须存在、清单键必须在原型上、原型新增公开成员必须被分类（派生算子必须逐维度登记覆盖资产，或显式写 `UNCOVERED`/`NA:<构造性理由>`）。
- **禁止把无覆盖的格子谎报为有资产**；`UNCOVERED` 是显式登记的债务，其汇总是每轮深度 review 的第一立项来源（先盘点盲格，再发明新方法）。
- 新增对抗值域维度时：生成器加进 `__tests__/fuzzKit.ts`（所有 fuzz 共享，"加一次、全算子可用"），清单加一列，全部算子重新过账。禁止在单个 fuzz 文件里私藏值域。
- 每轮 review 至少消掉一批盲格或给出 NA 论证；账本只允许经论证的增长（新算子落地时）。

### 4. data0 特有的 review 检查（附资产追溯）

每条检查项必须指向实现它的常驻资产；新增检查项而不补资产，视为该项未完成。

- 立案前先对照下方「架构决策与已知语义边界」一节：其中列出的行为是既定架构语义，观察到相关现象时引用该节说明，不得作为缺陷报告或擅自"修复"。
  资产：`__tests__/architectureSemantics.spec.ts`、README「架构语义」节。
- `RxList` 派生算子必须验证：每步增量结果等于从当前 `source.data` 全量重算的结果，**含 batch/延迟调度下多条 triggerInfo 单次 digest 重放的序列**（triggerInfo 的 key/argv 是操作时位置，source.data 是重放时终态，patch 端凡按终态解释操作时位置都是缺陷；无法安全增量时用 `return false` 回退全量重算，并同步 README 支持矩阵的脚注）。
  资产：`__tests__/broadOperatorsFuzz.spec.ts`（全算子差分）、`__tests__/duplicateValuesFuzz.spec.ts`（重复值域差分）、`__tests__/batchReplayFuzz.spec.ts`（batch 多操作重放差分 + toSorted 等值 tie 差分）、`__tests__/lifecycleAndReplayFixes.spec.ts`（最小复现回归）。README 的支持矩阵中每个"增量"格子必须有差分覆盖。
- mutation 测试至少覆盖 splice 的负数、越界、小数、`NaN`、`-0`，重复值，`set`，`reorder`，batch，以及回调抛错。
  资产：`broadOperatorsFuzz` 的操作生成器（对抗参数域）、`batchReplayFuzz` 的 batch 操作生成器、`__tests__/reproducedIssuesFixes.spec.ts`、`__tests__/reviewFixes.spec.ts`。
- 响应式回调变更后，除结果外还要验证依赖仍会触发、被删除 effect 已销毁、全局 tracking/session 栈恢复。
  资产：dev 不变量断言（`batch`/effect run 的栈深复原、digest 静止态、`RxList` 行级记账对齐——违约当场抛错，被全套测试被动执行）、`__tests__/invariantAssertions.spec.ts`（断言开火自检）。
- destroy 语义必须对称：destroy 后不再接收更新（僵尸检查）、create+destroy 后活跃 effect 计数回到基线（泄漏检查）、源模式结构与计算模式一视同仁（destroy 事件、children、惰性 meta、`context.onCleanup`）。子类的销毁清理必须放进 `destroyResources` 钩子（唯一会被所有销毁入口——实例 `destroy()`、`destroyChildren`、`destroyComputed`——执行的位置），不得放在 `destroy()` 覆写里。已销毁结构的变更方法是 no-op。
  资产：`__tests__/destroySemantics.spec.ts`（全派生族僵尸/泄漏横扫）、`__tests__/lifecycleAndReplayFixes.spec.ts`（destroy 取消在途 async patch、no-op 变更）。
- async patch/getter 的并发行为必须经交错枚举验证，不允许只测"启动→等待→断言终值"的单一顺序；destroy 与在途 async patch 的交错也在此列。
  资产：`__tests__/asyncPatchInterleavings.spec.ts`（两个 async patch + 中途写入的全排列）、`__tests__/lifecycleAndReplayFixes.spec.ts`（destroy × 挂起 patch）。
- 涉及 axii/axle 的 `triggerInfo.argv` 原始参数契约时，不得直接修改外部协议；内部消费者应独立归一化。
  资产：`deepReviewFixes.spec.ts`（argv 透传契约测试）、README「RxList 参数契约」节。
- 消费 `triggerInfo.oldValue`/`newValue` 的派生结构不得用 `!== undefined` 判断"有无"——undefined 是 RxList 的合法元素值，协议里"值为 undefined"与"无值"带内不可区分。无法按 key/methodResult 定位时回退全量重算（toSorted 先例）。加性 `hasOldValue` 字段的提案经 2026-H2 评估暂不引入：回退已保证正确性、无消费者需要 undefined 的增量处理，单方面加协议面属投机设计；若未来出现真实需求，须与 axii/axle 同步做加性扩展。
  资产：`deepReview2026H2Findings.spec.ts`（undefined 值域 sweep）、本条规则。

### 5. Review 输出格式

每份深度 review 至少包含：

- **已动态复现**：最小复现、实际/预期、影响范围。
- **已静态确认**：静态证据及为什么不适合或尚未进行运行复现。
- **待验证**：还缺少什么证据。
- **验证基线**：测试、类型检查、构建及其真实结果。

不得使用“所有问题均已复现”之类的总括表述，除非报告中的每一项和每个影响声明确实都有对应的执行证据。

## 架构决策与已知语义边界（不作为缺陷处理）

以下三项是与 data0 架构绑定的**既定语义**。它们都曾在深度 review 中被动态复现并完整评估过修复方案，结论是：修复与当前架构冲突过大（等价于重写传播引擎或推翻性能契约），维护者明确决定**不修**。

约定：

- Review 中再次观察到这些现象时，归类为"架构语义"，引用本节即可；不得作为缺陷立案，不得提交"修复"。
- `__tests__/architectureSemantics.spec.ts` 用特征测试（characterization test）固定这些行为。测试失败时先判断：无意的行为漂移 → 修代码恢复测试；有意的架构变更 → 必须与维护者确认方向，并同步更新本节与该测试。
- 每项都给出了"仍属缺陷"的边界：边界内的问题照常按证据纪律立案。

### A1. 急切推模式允许菱形依赖 glitch

- **语义**：atom 写入后按订阅顺序同步推播（`Notifier.trigger`/`triggerEffects`），无拓扑排序、无读时拉取。菱形依赖（`a→c` 且 `a→b→c`）下，先订阅的下游会以"新 a + 旧 b"先算一遍：可观察到系统从未处于的中间值，并产生重复重算。终值必然收敛正确。
- **不修的原因**：computed 急切求值与 `applyPatch` 的"触发即逐条重放 triggerInfos"都建立在推模式上。glitch-free 需要拉模式惰性求值（Vue 3）、拓扑排序调度（MobX）或版本校验（preact signals），任一方案都等于重写传播引擎，并推翻 batch/immediate/增量 patch 的现有语义。
- **仍属缺陷**：终值不收敛或终值错误；同一 effect 在单次无环传播中被无限触发；`batch()` 结束后结果与全量重算不一致。

### A2. `batch()` 内读 computed 返回进入 batch 前的值

- **语义**：effect session 把订阅者的执行推迟到 digest，"标脏"（`handleTriggered` 中置 `STATUS_DIRTY`）也随 run 一起被推迟；而 computed 的读路径没有"脏则重算"的拉取。因此 batch 内"先写依赖、再读该依赖的 computed"读到的是旧值。atom 本身的读取不受影响（写入立即生效，推迟的只是订阅者）。
- **不修的原因**："推迟副作用"是 batch 的存在意义；读时拉取与推模式核心冲突（同 A1）。`autorun`/`once` 已通过 `preventEffectSession` 独立保证自身场景的读写一致。
- **仍属缺陷**：batch **结束后**读取仍是旧值；batch 内读 atom 本身拿到旧值；digest 丢弃排队的 effect 导致永久陈旧。

### A3. 构造与 `replace` 采纳外部容器引用（所有权移交）

- **语义**：`new RxList(arr)`、`new RxSet(set)`、`new RxMap(map)`、`RxSet.replace(set)` 直接持有传入容器（零拷贝，`list.data === arr`）。调用方视为移交所有权：之后必须通过 Rx 实例的方法修改；绕过方法直改原容器不会触发任何通知，派生结构与源静默失联属于**契约内行为**。
- **不修的原因**：data0 没有 Proxy 包裹，响应性完全建立在"方法即变更边界"上；无条件防御性拷贝违背大数据量场景（十万行 `replaceData`）的性能目标。
- **仍属缺陷**：通过 Rx 方法修改却漏触发或触发错误；新增采纳外部引用的入口而未在注释中声明所有权移交。

## 常用验证命令

```bash
pnpm install --frozen-lockfile
pnpm test --run
pnpm test:prod        # 生产语义差分(__DEV__:false,排除 dev 特化文件)
pnpm type-check
pnpm build
pnpm exec vitest run --coverage
```

锁文件安装失败本身应作为仓库问题报告，不能静默改用无锁安装后声称 CI 基线正常。

以上基线由 CI（`.github/workflows/ci.yml`）在每个 push/PR 上强制执行。热路径改动必须附 `pnpm bench` 对比（分支 vs main）。

## Mutation 审计（周期性，不进 CI）

行覆盖率不度量测试的检出能力（95% 覆盖曾对多个致命缺陷零预警）。用 mutation testing 度量"测试套件到底能杀死多少注入错误"：

```bash
# 限定单模块审计（全量运行过慢，按模块轮换；默认配置见 stryker.config.json）
pnpm mutation                                    # 默认 mutate src/util.ts
pnpm exec stryker run --mutate 'src/dep.ts'      # 指定其他模块
```

- 建议在每轮深度 review 前对本轮重点模块跑一次，幸存的 mutant 即测试盲区，优先补差分/性质测试而不是逐 mutant 补例子。
- **排期按风险而不是按轮换**：体积 × 分支密度 × 距上次审计时间。RxList.ts（最大、分支最密的模块）2026-H2 首跑即暴露 479 个幸存 mutant——绝对量远超先审的小模块，验证了风险优先的必要性。
- 幸存 mutant 涉及「架构决策」语义的，对照该节判断是否属于刻意未断言的行为；命中长期无人使用的辅助代码（如 Vue 继承的字符串工具）时，优先考虑删除死代码而不是补测试。
- 基线记录（用于观察趋势）：
  - 2026-07 首跑 `src/util.ts` —— 行覆盖 98.66%，mutation score **65.94%**（240 killed / 113 survived / 12 no-coverage，48s）。行覆盖与检出能力的差距即测试盲区的量化。
  - 2026-07 修复轮跑 `src/reactiveEffect.ts`（destroy 核心重构后）—— 行覆盖 96.1%，mutation score **73.10%**（209 killed / 3 timeout / 71 survived / 7 no-coverage，94s）。
  - 2026-07 评估修复轮：删除 Vue 遗留且无生产引用的字符串/指令辅助（`isOn`/`camelize`/`isReservedProp` 等），降低 util 中"无人使用却撑高幸存 mutant"的死代码面。
  - 2026-07 方法 10 轮首跑 `src/RxList.ts` —— mutation score **72.08%**（1401 killed / 37 timeout / 479 survived / 78 no-coverage，4m52s；测试套为 selection/undefined 修复后、duplicates/undefined 扩展 sweep 落地前）。479 个幸存 mutant 为当前最大单模块盲区账本，下轮 review 与 coverageInventory 的 UNCOVERED 格子共同作为立项来源。
