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
  12. NaN/-0 元素值域差分（weirdNum 列）+ 核心表面账本化 + patch 错误恢复探针。动态复现并修复三个缺陷类：toSorted 增量删除按 `===`/`indexOf` 定位在 NaN（找不到→静默残留）与 -0/0（tie 组内命中错误实例）下与全量分叉——修复为 `Object.is` 身份定位 + 定位失败/tie 组含可区分成员时回退全量；applyPatch 抛错后 phase 停留在 PATCH_PHASE、抛错轮 info 已消费——下次触发只增量重放新 info，抛错轮变更永久缺失——修复为 handleRecomputeError 统一回退 FULL_RECOMPUTE_PHASE；findIndex 的 splice 分支用终态长度回推"操作时长度"归一化负/越界 start，多 info 重放下回推失效、被删除的旧 match 静默保留（fuzzKit 统一对抗参数域后由 batchReplayFuzz 命中）——修复为多 info 一律回退全量（等价类同 groupBy/slice，README 脚注已更新）。契约裁定：toSorted comparator 必须构成一致全序（NaN × 裸数值 comparator 属契约外），README 参数契约节已更新。资产：`__tests__/weirdNumbersFuzz.spec.ts`（NaN/-0 值域差分）、`__tests__/coreLifecycleGaps.spec.ts`（async/generator getter × destroy、排队后 destroy、三参调度器契约、调度错误兜底、patch 错误恢复、oncePromise 拒绝、object-atom 协议特征）、`__tests__/coreSurfaceInventory.spec.ts`（核心表面账本 + 导出面普查）、`__tests__/lifecycleAndReplayFixes.spec.ts`（findIndex 多 info 最小回归）。
  13. 消费者契约回放（consumer contract pinning）：读取 axii/axle 宿主（RxListHost）对 triggerInfo 的真实消费面（splice 的 argv 原始透传/methodResult 删除项、reorder 的 argv[0]+reorderInfo 结构、EXPLICIT_KEY_CHANGE 的 key 透传与 methodResult 旧值、RxSet add/delete 的 argv 与 replace 的 [newItems,deletedItems]、RxMap set/delete/clear/replace 的 methodResult 形状），在 data0 侧钉成协议形状测试——改动字段形状在本仓 CI 当场失败，而不是等下游升级才发现断链。**反向回放（下游侧，2026-H2 完成）**：以 sibling checkout 在 axii/axle 各自仓内对 2.9 语义跑全套 + 契约测试。发现并修复 axii 真实缺陷：本仓缺陷类 4 的修复（patch 抛错后重跑 computation）打破了 axii RxListHost「computation 只跑一次」假设——重跑时残留 hosts 令行重复创建、撞 "should never rerender" 断言，列表区域永久崩坏（axii#37 修复 + 双代契约资产）；axle 因 applyPatch 全部就地消化错误、永不上抛而构造性免疫，该前提已在 axle#25 钉成契约。教训：**行为语义（非字段形状）的变更同样需要双向契约资产**，axii 的 data0Contract.spec 条款 8 与本仓的错误恢复语义现在互相锁定。同轮账本燃尽：核心账本 13 格清零（generator patch 全维、generator getter 错误/交错/batch、asyncGetter batch、objectProxy batch、onChange 错误/batch、children batch、probe 错误注入），集合账本关闭 undefined × RxMap/RxSet/selection、weirdNum × selection、batchReplay × reduce/indexBy/toMap/selection、destroy × toMap/createSelections/RxSet.has/size/isSupersetOf；RxTime 修复多入口 disposer 覆盖泄漏（resolve+subscribe 混用时先注册的 autorun 永久泄漏，第一轮静态确认的遗留缺陷）并以 fake-timer 确定性审计取代真实时钟依赖。资产：`__tests__/consumerContractReplay.spec.ts`、`__tests__/coreLedgerBurndown.spec.ts`、`__tests__/collectionLedgerBurndown.spec.ts`、`__tests__/rxTimeDeterministic.spec.ts`。
  14. 增量性见证（incrementality witness）+ 债务清仓轮：`fullRecompute` 见证事件把「增量 ≠ 静默回退」变成可断言语义，README 支持矩阵首次获得双向可执行定义（增量格子零回退 + 重算格子必回退），直接猎杀 mutation 审计暴露的最大幸存类；同轮修复缺陷类 7（toMap/属性形式 indexBy × OOB set：`undefined` 行解构/属性读 TypeError 且派生链永久毒化，违反稀疏 sweep 等价类——修复为洞位行统一跳过，全量与 patch 两侧一致）；集合账本 33 格清零（weirdNum × RxSet 代数差分 fuzz、undefined/NaN × indexBy/toMap/reduce/selections/RxSet 谓词、sparseOOB × toMap/reduce/createSelections、batch × RxSet 谓词）；外围模块补齐（AsyncRxSlice 的 receipt 交错/错误路径/销毁、LinkedList 的结构操作/记账清理/响应式迭代）。资产：`__tests__/incrementalityWitness.spec.ts`、`__tests__/collectionLedgerBurndown2.spec.ts`、`__tests__/peripheralModules.spec.ts`。
  15. 幸存 mutant 语料驱动的语义钉扎（survivor-corpus pinning）+ 死代码清偿：把 PR #9 显式在账的"439+146 幸存 mutant 渐进消化"作为立项对象，重跑两模块审计后**逐 mutant 分类**幸存语料，区分「安全方向变异」（回退强制/范围放宽/快慢路径选择/防御分支——结果仍正确，构造性不可杀，见下方基线记录的等价类裁定）与真实检出盲区，只对后者补资产：契约守卫可达性（reposition/swap 越界、indexBy/toMap 重复 key、destroyed clear/set/reorder no-op）、reorderInfo 协议**字段值**钉扎（consumerContractReplay 只钉过形状）、at(index) 精确触发的有界穷举（触发不多/不少/升序——差分 fuzz 只对比终值，从不检查"谁被通知了"）、slice 区间算术穷举 + 增量性边界（start=0 窗口、负边界 set 不回退）、groupBy × NaN key、filter/slice 幽灵触发钉扎、async 打断的 asyncStatus 转换序、generator 过期轮段级丢弃、recompute(force) 越过 PATCH 阶段、markDirty 传播、updatedAt 惰性同步、dev 不变量开火自检扩展（atomIndexes 漂移/越长、行 frame 错位）。同轮死代码清偿：findIndex 的逐项增量 cache（被 hasItemReactiveDeps 全量回退门构造性遮蔽，36 个 no-cov mutant 的来源，热路径每步搜索白付一次对象分配）、Computed 的 keyToEffectFrames/hasDeps/getCachedValue/实例 collectEffect、util 的 uuid/isDate/isRegExp/getStackTrace/hasOwn/isString/isSymbol/isObject/isFunction/isPromise/toRawType（全部零生产引用，仅覆盖率测试喂养）。资产：`__tests__/mutationKillersRxList.spec.ts`、`__tests__/mutationKillersComputed.spec.ts`、`__tests__/invariantAssertions.spec.ts`（扩展）。
  16. 宿主语义差分回放（host-semantic differential replay）+ 核心平面重入边界：按 axii/axle RxListHost 真实消费面维护平行 `hosts[]`，断言每步（含 batch 多 info、派生链作 hosted source、回调重入、digest 内 batch、嵌套 batch、中途 destroy sibling）`hosts ≡ source.data`。对照"axii 现状"（EKC 读 `source.data[key]`）与"协议正确"（`info.newValue`）：前者在 batch `set`+结构操作下动态分叉，后者全程对齐——协议字段充足，分叉属下游消费脚枪。同轮动态复现并修复：`isPlainObject` 用 `constructor` 门控导致 `Object.create(null)`/`constructor` 覆写后 object-atom 属性读失明（改原型链判定）；AsyncRxSlice destroy 不抬升 receipt 致在途 reject 僵尸写 `loadError`（destroyResources bump receipt + 复位控制 atom）。嵌套 batch/digest 内 batch、mid-recompute create/destroy、RxTime 多入口清理未发现反例。资产：`__tests__/deepReview2026H3Findings.spec.ts`、`__tests__/_coreReview16*.spec.ts`；README「RxList 参数契约」补 EKC 须消费 `info.newValue`。
  17. 身份别名差分（identity-aliasing differential）：同一对象引用出现在 RxList/RxMap/RxSet/LinkedList 多位置，经一路径变更后与全量重算/原生结构对账。动态复现并修复 `groupBy` 空组键残留（增量 remove/set 清空组后只清内容不删 map 键，`has`/`size`/`keys` 与全量分叉；既有 fuzz 只断言已有键的内容，空≡空通过）。同轮：LinkedList 重复身份下 `removeBetween` 误删幸存者 WeakMap 条目（仅当映射指向本节点才删）；`spliceMany` 大数组路径 × `NaN` deleteCount 与原生分叉但当前公开面不可达。别名 × toSet/filter/map/selection/RxMap 未发现反例。资产：`__tests__/deepReview2026H3Findings.spec.ts`；fuzz 族补 groupBy **键集**断言（broad/duplicate/batchReplay/weirdNumbers/modelComparison）。
  18. 协议命名空间碰撞审计 + 真实 GC 可达性/记账残留审计（2026-H3 第二轮）。(a) 把「用户数据值域 ∩ 内部协议常量」当对抗值域：METHOD/EXPLICIT_KEY_CHANGE 订阅曾以字符串枚举值直接作 depsMap key，RxMap key / RxSet 成员 / groupBy 组键 / indexBy·toMap 键恰为 `'method'`/`'explicit_key_change'`（HTTP method 分组是现实输入）时，SET/ADD/DELETE 的 key dep 与协议订阅者同 dep——RxMap.keys 的 assert(unreachable) 抛给 set/delete 调用方、RxSet.toList/代数族解构 TypeError 且静默分叉。修复为 track/trigger 双侧 Symbol 隔离（`METHOD_TRACK_KEY`/`EXPLICIT_KEY_CHANGE_TRACK_KEY`，公开 manualTrack 调用形状不变，构造性关死等价类）。同轴：trigger 的 `key !== void 0` 把「key 恰为 undefined」当「未提供 key」，`RxMap.get(undefined)` 订阅者永久漏触发（改 `'key' in inputInfo`）；`RxMap.set` 的 `===` 判等与库内 Object.is 身份语义分叉（0→-0 已写入却不触发、NaN→NaN 重复触发）。(b) WeakRef × `--expose-gc` 的真实堆审计（retainedDiagnostics 是计数器、measure-retained 是称重，都不证 GC 可达性）：destroy 后派生结构全部可回收 ✓；但「订阅不同 key → 退订」churn 在长活 source 的 depsMap 留下无界空 Dep 条目（10000 次 `at(i)`/`get(key)` 循环 → 10000 条残留）——修复为 dep 记录 host/hostKey、销毁路径退订到空即摘除（cleanup 仅 `!active` 时摘：活跃重算的复位路径零开销，findIndex patch 曾因无差别摘除 -13%），async asyncTracks 重放与 restoreEffectDeps 两条重添加路径挂回宿主或并入现行 dep，`pruneIndexKeyDeps` 与 depsMap 重同步；审计还暴露 destroy 后完成的 async/generator getter 把僵尸 effect 重订阅回 dep（asyncTracks 重放无 active 门，泄漏 + 幽灵调度）。(c) 顺带（操作时位置 × 重放终态的账内清偿）：reduce/reduceToAtom 的「纯尾插」判定按终态长度回推，多 info 下「越界 clamp 尾插」误入增量且 index 分叉——判定与应用改走 digestReplay 操作时长度，batch 连续 push 由此保持增量（README 脚注已更新）。热路径核验：定向 ABBA 微基准（15 轮取中位）write-atom +0.1%、findIndex patch −0.6%、map patch +1.4%、at() −2.5%，均在环境噪声内（vitest bench 的 main-vs-main 噪声中位 3.5%、最大 16.6%，不足以裁定本级别改动）。资产：`__tests__/deepReview2026H3Round2.spec.ts`（六个缺陷类的实例回归 + 等价类横扫）、`scripts/audit-reachability.mjs`（可回收性 + 记账有界性，周期性运行，不进 CI）。

  19. 创建时形态假设 × 运行时形态迁移横扫（shape-migration sweep，2026-H3 第三轮）+ notify.ts/RxSet.ts 首轮 mutation 审计。(a) 攻击轴：多个结构的运行时「形态」在创建时一次性定型——atom 的 primitive/proxy 形态由 initValue 决定、派生列表的段偏移/index 校正区间算术假设源保持稠密、RxTime 的定时器排定假设线性表达式非退化；让值/形态在运行时迁移穿过这些创建时假设（primitive atom 写入对象、稠密列表经 OOB set 变稀疏后走校正/分段路径、RxTime 系数相消）。既有稀疏 sweep 只覆盖「稀疏 × 纯尾插」——尾插不进任何按区间遍历的校正路径，盲格在「稀疏 × 不等长 splice / reorder / EKC」。动态复现并修复四个缺陷类：createIndexKeySelection 两条 index 校正循环（不等长 splice 平移区、reorder affectedRange）撞洞位行 TypeError 直接抛给 list.splice/swap 调用方——违反「OOB set × 派生算子不崩溃且可恢复」等价类，修复为 ?. 跳洞（与 map/filter 行级守卫一致）；concat 的 EKC 分支对越界 key（段长跳变）按段内偏移直写，跨段覆盖后续源的段（B 段整体错位，结构性错乱而非洞物化差异）——修复为 key ≥ 旧段长（本列表长 − 其他源现长）回退全量；RxMap.replace 的 SET 触发缺 Object.is 判等门与 oldValue 字段——方法 18 修 set() 的同一等价类漏网（**判等门必须覆盖同一变更语义的所有入口**），整表 replace 幽灵触发全部 get(key) 订阅者；RxTime 系数相消（coefficient=0）时 -constant/0=±Infinity → setTimeout(Infinity)，Node 打 TimeoutOverflowWarning 并 clamp 成 1ms 虚假唤醒——修复为 isFinite 守卫。特征钉扎（契约边界，README「传播模型」已同步）：atom 对象特性由创建时 initValue 形态定型，primitive→object 值迁移后属性级读写静默落在 atom 函数对象上（不写 value、不触发；axii atom(null) 惯用法的整值替换不受影响）；class 实例 atom 属性写（写穿实例+触发）与属性读（不转发）不对称。稀疏残留分叉（groupBy reorder 分支 filter 跳洞 vs 全量物化洞、toSet 洞成员物化差）经评估归入「洞的物化语义」既有契约外债务，不崩溃可恢复，未立案。(b) notify.ts 首轮 mutation 审计（见基线记录）＋死代码清偿：triggerEffects 的 CompactDep 快路径为不可达死分支（CompactDep 只挂在 primitive atom 函数对象上、派发一律走 triggerPrimitiveAtomValue 特化路径，从不进 targetMap），删除；真实检出盲区补杀 3 个（triggerPrimitiveAtomValue 的 shouldTrigger 暂停门、scheduleAtomEffect 的 session info 形状与逐次累积、多 dep 去重的恰一次派发）。(c) 下游反向确认：axii（sibling alias，53 文件 672 tests）、axle（临时 alias data0→HEAD + __DEV__ define，33 文件 459 tests）对修复后 HEAD 全绿；热路径核验：triggerEffects 死分支删除经定向 ABBA 微基准（object-atom 写 ×200k + splice ×30k，15 轮取中位）−0.99%，噪声内。资产：`__tests__/deepReview2026H3Round3.spec.ts`（四缺陷类实例回归 + 形态迁移特征钉扎 + notify 盲区补杀）、`__tests__/sparseSetOperatorsSweep.spec.ts` 校正路径横扫（不等长 splice + swap × 全算子族，洞位行等价类）。
### 3.3 覆盖清单纪律（盲格必须可见）

2026-H2 教训：两个存活多轮的缺陷类都落在"攻击轴已存在、但没和该算子/值域相乘"的盲格上（重复值域 × selection、undefined 值域 × toSorted）。方法轮换有清单，覆盖矩阵却没有清单，盲格因此不可见。规则：

- `__tests__/coverageInventory.spec.ts` 是「公开派生算子 × 对抗维度 → 防线资产」的常驻账本，机械强制：引用的资产文件必须存在、清单键必须在原型上、原型新增公开成员必须被分类（派生算子必须逐维度登记覆盖资产，或显式写 `UNCOVERED`/`NA:<构造性理由>`）。
- `__tests__/coreSurfaceInventory.spec.ts` 是响应式核心（computed/notify/reactiveEffect/atom）的「计算表面 × 对抗维度」账本 + **包导出面普查**：src/index 的每个运行时导出必须被分类（核心表面/集合/外围/工具/诊断），新增导出未分类当场失败；核心表面按 basic/errorInjection/destroyTiming/interleaving/batchSession/scheduler 六维登记。两本账本的 `UNCOVERED` 汇总共同构成 review 立项来源。
- **禁止把无覆盖的格子谎报为有资产**；`UNCOVERED` 是显式登记的债务，其汇总是每轮深度 review 的第一立项来源（先盘点盲格，再发明新方法）。
- 新增对抗值域维度时：生成器加进 `__tests__/fuzzKit.ts`（所有 fuzz 共享，"加一次、全算子可用"），清单加一列，全部算子重新过账。禁止在单个 fuzz 文件里私藏值域。
- 每轮 review 至少消掉一批盲格或给出 NA 论证；账本只允许经论证的增长（新算子落地时）。
- **Oracle 完备性纪律（2026-H3 教训，对偶于 UNCOVERED 规则）**：差分比较默认走 `__tests__/stateOracle.ts` 的**全可观察状态**规范化比对（键集 + 逐键内容 + size），禁止手写"只遍历已存在键比内容"式的部分投影——groupBy 空组键残留曾因该投影（空 ≡ 空恒真）在五个 fuzz 资产下存活多轮，方法 11 的朴素参考模型也被同一投影消费掉。任何弱化（只比部分面）必须在断言处显式注释理由；新增派生结构类型先在 stateOracle 补规范化器再写 fuzz。输入维度有账本（coverageInventory），观察维度同样要有清单：oracle 的强度决定差分测试的上界。

### 4. data0 特有的 review 检查（附资产追溯）

每条检查项必须指向实现它的常驻资产；新增检查项而不补资产，视为该项未完成。

- 立案前先对照下方「架构决策与已知语义边界」一节：其中列出的行为是既定架构语义，观察到相关现象时引用该节说明，不得作为缺陷报告或擅自"修复"。
  资产：`__tests__/architectureSemantics.spec.ts`、README「架构语义」节。
- `RxList` 派生算子必须验证：每步增量结果等于从当前 `source.data` 全量重算的结果，**含 batch/延迟调度下多条 triggerInfo 单次 digest 重放的序列**（triggerInfo 的 key/argv 是操作时位置，source.data 是重放时终态，patch 端凡按终态解释操作时位置都是缺陷）。多 info 重放**必须经 `src/digestReplay.ts` 内核**取每条 info 操作时的源状态快照（构造性关死该缺陷类；2026-H3 起 groupBy/slice/findIndex 由此从"多 info 回退全量"升级为增量），禁止在 patch 里手写终态回推；内核判不可重建或语义仍无法增量时用 `return false` 回退全量重算，并同步 README 支持矩阵的脚注。
  资产：`__tests__/digestReplay.spec.ts`（内核逆操作 + batch 内逐操作实录的差分地面真值）、`__tests__/broadOperatorsFuzz.spec.ts`（全算子差分）、`__tests__/duplicateValuesFuzz.spec.ts`（重复值域差分）、`__tests__/batchReplayFuzz.spec.ts`（batch 多操作重放差分 + toSorted 等值 tie 差分）、`__tests__/lifecycleAndReplayFixes.spec.ts`（最小复现回归）。README 的支持矩阵中每个"增量"格子必须有差分覆盖。
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
- 内部记账 key（depsMap、行级 Map 等）不得与用户数据值域共享命名空间：协议订阅一律用内部 Symbol（`METHOD_TRACK_KEY`/`EXPLICIT_KEY_CHANGE_TRACK_KEY`/`ITERATE_KEY` 先例），新增"以固定值为 key 的订阅面"必须用 Symbol 而不是字符串常量；带内合法值（undefined、NaN、协议同名字符串）必须能作为普通数据 key 正常工作。
  资产：`deepReview2026H3Round2.spec.ts`（协议字符串 × RxMap/RxSet/groupBy/indexBy/toMap/toSet/selection 横扫 + undefined key 回归）。
- 长活 source 上的订阅记账必须有界：谁创建记账条目，谁负责在退订到空时回收（depsMap 的 host 摘除、`pruneIndexKeyDeps` 的重同步）；新增"按 key 建 dep/缓存"的结构时必须给出回收路径，并用 `scripts/audit-reachability.mjs` 式的 churn 探针验证有界性。destroy 后完成的 async 收尾不得重放订阅（active 门）。
  资产：`deepReview2026H3Round2.spec.ts`（churn 有界 + 错误恢复挂回 + 僵尸重订阅回归）、`scripts/audit-reachability.mjs`（真实 GC 审计）。

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
  - 2026-07 核心账本轮首跑 `src/computed.ts` —— mutation score **68.04%**（369 killed / 10 timeout / 149 survived / 29 no-coverage，3m17s；coreLifecycleGaps 资产落地前）。核心模块的检出率低于集合模块，与"核心平面此前无账本"的判断相互印证。
  - 2026-07 燃尽轮重测（coreLedgerBurndown/collectionLedgerBurndown/consumerContractReplay/weirdNumbersFuzz 落地后）——`computed.ts` **70.13%**（381 killed / 11 timeout / 146 survived / 21 no-coverage，+2.09pt，no-cov 29→21）；`RxList.ts` **72.19%**（1440 killed / 37 timeout / 490 survived / 79 no-coverage，+0.11pt；总 mutant 数随新代码路径增加，killed +39）。账本燃尽对核心模块的检出提升显著、对 RxList 的边际提升有限——RxList 的 490 幸存 mutant 需要按等价类专项消化（差分 fuzz 的断言粒度问题），而不是继续加维度，此为下轮立项方向。
  - 2026-07 幸存 mutant 等价类分析（490 个全量分类）：**最大等价类（约 70%，ConditionalExpression 222 + EqualityOperator 91 + LogicalOperator 28，集中在 slice/map/doSplice/toSorted/concat 的增量路径条件与区间算术）= "把增量 patch 变异成回退全量/等价慢路径"——结果仍 ≡ 全量重算，差分 fuzz 构造性杀不死（差分只证结果不证增量性）**。对策：`Computed` 增加 `fullRecompute` 见证事件（无监听者零开销），新资产 `__tests__/incrementalityWitness.spec.ts` 对 README 矩阵逐格断言「增量格子契约内单 info 操作零回退 + 重算格子回退确实发生」，双向钉死增量性语义。次大类：StringLiteral 30（断言/警告消息文本，行为等价，接受不杀）；其余 BlockStatement/OptionalChaining 多为防御性清理路径，由 destroy/稀疏 sweep 渐进覆盖。
  - 2026-07 见证资产落地后重测 `RxList.ts` —— mutation score **74.93%**（1507 killed / 41 timeout / 439 survived / 79 no-coverage，+2.74pt，killed +67，survived 490→439）。增量性见证单资产杀掉 51 个幸存 mutant，验证等价类判断；剩余 439 中约 30 为行为等价（消息文本/性能快慢路径选择），其余作为渐进债务随各 sweep 演进。
  - 2026-07 `src/digestReplay.ts` 内核落地跑 —— 首跑 70.00%（91 killed / 38 survived / 1 no-cov）；补"协议外合成 info 直喂内核"的守卫可达性测试后 **83.85%**（109 killed / 20 survived / 1 no-cov，30s）。剩余幸存为防御等价类：`methodResult ?? []` 与可选链（协议内恒存在）、reorder 越界检查的单子句变异（其余子句仍拦截）、`new Array(n)` 容量提示（按索引赋值语义等价）——均归入「安全方向/防御分支」接受项。
  - 2026-07 方法 18 轮首跑 `src/dep.ts`（host 记账落地后）—— mutation score **61.49%**（90 killed / 1 timeout / 38 survived / 19 no-coverage，67s）。dep.ts 此前从未单独审计；幸存主体分类：CompactDep 的溢出/降级路径子句（升级 Set/收缩回 single 的边界，由 primitive atom 行为间接覆盖但无专项 killer）、Vue 继承的 dep-marker 机制（wasTracked/init/finalize 的位运算子句——行为由全套 computed 测试被动钉住，单子句变异多为等价慢路径）、新增 host 记账的防御分支（`reattachDepToHost` 的 merge 分支与 `pruneEmptyDepFromHost` 的身份检查：当前调用序下构造性接近不可达——cleanup 的 prune 门控在 `!active`、marker 模式无 pre-cleanup——保留是防未来调用序变化，归入方法 15 裁定的「防御分支」接受项）。
  - 2026-07 方法 19 轮首跑 `src/notify.ts`（此前从未单独审计）—— mutation score **68.90%**（288 killed / 100 survived / 30 no-coverage，3m42s）。幸存分类：no-cov 主体 = triggerEffects 的 CompactDep 快路径（**不可达死分支**——CompactDep 只挂在 primitive atom 函数对象上，派发一律走 triggerPrimitiveAtomValue 特化路径，从不进 targetMap；本轮删除，ABBA 微基准 −0.99% 噪声内）+ track/trigger 监听者派发 payload（诊断面）；survived 主体 = dev 断言子句/消息文本（类 d）、快慢路径选择（空 session 快出口、hasMultiple 去重升级、normalizeTrackKey 先比 type 的短路，类 a）、防御分支（digest finally 复位循环、getDepEffects 空守卫，类 d）、被内部调用形状遮蔽的守卫（协议 key 归一化对内恒等、activeEffect 自触发抑制的逐派发形态变体，类 c）。真实检出盲区 3 个已补杀：triggerPrimitiveAtomValue 的 shouldTrigger 暂停门（既有覆盖只走 keyed trigger）、scheduleAtomEffect 的 session info 形状与逐次累积（既有 batch × atom 覆盖都用不消费 info 的 effect）、多 dep 命中的恰一次去重派发。死代码删除 + 补杀后重测 **72.61%**（289 killed / 97 survived / 12 no-coverage）。
  - 2026-07 方法 19 轮首跑 `src/RxSet.ts`（此前从未单独审计）—— mutation score **90.39%**（254 killed / 27 survived / 0 no-coverage，44s）。集合模块最高分；幸存全部有覆盖，主体为 destroyed no-op 警告文本、per-key info 字段（RxSet 无 key 级订阅面，字段无消费者而被遮蔽）与"delete 非成员误触发"类幽灵 info（下游 patch 幂等吸收），归入类 c/d 接受项，无检出盲区立案。
  - 2026-07 方法 15 债务消化轮。立项前重跑基线（环境漂移校准）：`RxList.ts` 74.70%（1523 killed / 39 timeout / 449 survived / 80 no-coverage）、`computed.ts` 70.18%（382 killed / 11 timeout / 146 survived / 21 no-coverage）。资产 + 死代码清偿落地后——`RxList.ts` **81.47%**（1617 killed / 41 timeout / 345 survived / 32 no-coverage，+6.77pt；no-cov 80→32 主要来自 findIndex 死缓存删除）；`computed.ts` **76.37%**（406 killed / 11 timeout / 119 survived / 10 no-coverage，+6.19pt，no-cov 21→10）。**等价类裁定修正**：此前"剩余约 30 为行为等价"严重低估——本轮逐 mutant 分类确认剩余幸存以「安全方向变异」为主体：(a) 把增量条件变异成"强制回退全量/强制慢路径"（结果仍 ≡ 全量重算，如 doSplice 的 isPureAppend/isPureClear 快路径与慢路径可观察等价、slice 的负 end 单 info 回退是保守而非必要）；(b) 把校正区间变异成"放宽为全量重写"（幂等写 + atom 判等去重使超集校正不可观察，如 createIndexKeySelection 的 affectedRange）；(c) 被构造性防线遮蔽的内部错误（binarySearchFind 的二分变异被 Object.is 线性兜底修正、atomIndexes 初值变异被后续校正循环覆写）；(d) dev 断言消息文本与断言子项（可达状态下其余子项仍然成立）。这些类**定义上不可由行为测试杀死**，属于接受项而非债务；真实债务（守卫可达性、协议字段值、触发精确性、状态机转换序）已由本轮资产钉住。后续轮次不应再以"分数逼近 100%"为目标，而以"新幸存 mutant 必须能归入上述四类之一，否则立案"为验收标准。
