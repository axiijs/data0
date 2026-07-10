# data0 性能优化方案(深度 Review 与实施计划)

> 状态:方案评审中,未实施。
> 基线环境:Node v22.14.0 / Linux x64,commit `ea73b4f`(v2.2.0),生产构建(`__DEV__: false`)。
> 本文所有行号以该 commit 为准。

---

## 1. 背景与目标

data0 是 axii / axle 渲染框架的响应式内核。下游的使用模式决定了性能敏感点:

- **axii 的每个响应式 DOM 绑定就是一个 `ReactiveEffect`**(`LightBindingEffect` 直接继承 `ReactiveEffect`,不走 `computed`/`autorun`)。列表渲染场景下 effect 的创建/销毁、atom 读(track)、atom 写(trigger)是最高频操作。
- 用户侧 JSX 中的 `list.map(...)` 会走 data0 的 `RxList.map`,per-row 的 `Computed`、`atomIndexes` 的成本直接体现在长列表首屏和滚动上。
- axle(canvas 引擎)自研 `BindingEffect`,同样绕开 `computed`,原因与 axii 一致:"没有 status/updatedAt atom、applyPatch"(见 axle `src/BindingEffect.ts` 注释)。**两个下游都在为绕开 Computed 的固定开销而自建轻量层,这本身就说明内核该瘦身了。**

本方案的目标按优先级排列:

1. **降低单次变更传播成本**(atom 写 → effect 重跑):目标 ≥2 倍吞吐,GC 分配减少一个数量级。
2. **消除结构性性能悬崖**:一次 `forEach` 永久拖慢列表所有后续 `splice` 这类问题。
3. **降低每对象常驻内存与创建成本**:`Computed`/`RxList`/`RxMap` 的固定开销。
4. 在不破坏"增量计算(patch)"这一核心卖点与公开 API 语义的前提下完成上述目标。

## 2. 基线数据(实测)

### 2.1 吞吐(`node scripts/measure-speed.mjs`)

| 场景 | 耗时 | 折算 |
|---|---|---|
| atom write+read,1 个订阅者,2M 次 | 492.7ms | **≈246ns/写** |
| atom read untracked(`.raw`),5M 次 | 3.7ms | ≈0.7ns/读 |
| effect create+track+destroy,200k 次 | 34.4ms | ≈172ns/个 |
| RxList splice churn(1000 行 ×200 轮) | 4.3ms | — |
| batch 写 100 atoms ×20k 轮 | 680.7ms | **≈340ns/写(比非 batch 还慢)** |

### 2.2 GC 压力

`--trace-gc` 下,2M 次带单订阅者的 atom 写触发 **1309 次 Scavenge(minor GC)**,即约每 1500 次写就要一次 GC。这直接印证了 §3 的分配审计:trigger 热路径每次写分配 6~8 个短命对象。

### 2.3 vitest bench(`bench/core.bench.ts`)

| 用例 | mean |
|---|---|
| write atom with 1 computed subscriber | 1.29µs |
| read 10 atoms inside computed recompute | 2.64µs |
| chain of 10 computed, single source write | **13.8µs(1.38µs/层)** |
| batch write 10 atoms with 1 computed subscriber | 5.4µs |
| create + destroy computed | 1.9µs |
| push+pop with map subscriber (1000 行) | 9.1µs |
| create + destroy RxList(100) with map | **157µs(≈1.5µs/行)** |
| at() read x100 inside computed (tracked) | 18.2µs(untracked 仅 2.5µs,**tracking 开销 ≈7 倍**) |

### 2.4 常驻内存(`node --expose-gc scripts/measure-retained.mjs`)

| 对象 | 字节/个 |
|---|---|
| primitive atom | 176B |
| primitive atom + 1 subscriber | 621B |
| LightEffect(无 deps) | 136B |
| **computed(1 个 atom dep)** | **1083B** |
| bare computed(无 deps) | 627B |
| **RxList(empty)** | **1793B** |

作为参照:同类 signal 库(Vue 3.5 ref / preact-signals / alien-signals)单次写传播在 20~80ns 量级、几乎零分配。data0 与之存在约 3~10 倍差距,差距主要来自**分配**而非算法复杂度——这是好消息,因为分配是可以系统性消除的。

## 3. 热路径审计:一次 atom 写的完整成本分解

以最常见路径为例:`a(1)`,a 是 primitive atom,订阅者是 1 个 immediate 的 sync `computed`。生产模式下的分配清单:

| # | 位置 | 分配 |
|---|---|---|
| 1 | `src/atom.ts:172` | inputInfo 对象 `{ key:'value', newValue, oldValue }` |
| 2 | `src/notify.ts:375` | `{...inputInfo, source, type}` 再拷贝一次(紧接着 `:376` 的解构只为 `__DEV__` 的 eventInfo,生产中是死代码) |
| 3 | `src/notify.ts:416` | `[...dep]`:CompactDep 的 `Symbol.iterator` 是 generator(`src/dep.ts:87`),展开 = 1 个 generator 对象 + 1 个数组 |
| 4 | `src/notify.ts:433` | `extend({ effect }, undefined)`:每个 effect 每次触发 1 个对象,即使没人监听 'trigger' 事件 |
| 5 | `src/reactiveEffect.ts:134` | `dispatch(event, ...args)` 的 rest 数组(每次 dispatch 都有,一次重算周期至少 dispatch 'trigger'/'dirty'/'recompute'/'cleanup'/'clean' 5 次) |
| 6 | `src/notify.ts:438` | `effect.run([info])`:单条 info 包一层数组 |
| 7 | `src/computed.ts:361/397/424` | 一次重算写 `status` atom 3 次(DIRTY→RECOMPUTING→CLEAN),每次写在 `atom.ts:172` 各分配 1 个 inputInfo(status 无订阅者时 `triggerPrimitiveAtomValue` 提前 return,但 inputInfo 已经分配) |
| 8 | `src/computed.ts:425` | `new Date().getTime()`:每次重算 1 个 Date 对象 |
| 9 | `src/computed.ts:386` | 走调度路径时 `[...(this._triggerInfos ?? [])]`,而内置的 `scheduleNextMicroTask/scheduleNextTick` 签名只有 2 个参数,第三个参数从不被消费 |
| 10 | `src/computed.ts:391` | 非 immediate 路径下每轮 dirty `createCleanPromise()`:1 个 Promise + 3 个闭包,即使没有任何人 await |

合计:immediate 链路上一次写 ≈ **12~16 个短命对象**;而这些对象大多数消费者(axii 的轻量绑定 effect)**根本不读**——`TriggerInfo` 只有 patch 型 Computed 才需要。

track 侧同样有一处热点:`src/notify.ts:247` 每次成功 track 都执行 `dispatch('track', { effect, ...debuggerEventExtraInfo })`,无监听者时白白分配 payload + rest 数组;`at()` 在 computed 内读 100 次的 bench 中 tracking 开销 7 倍于裸读,这是主要来源之一。

## 4. 优化项清单

按四个 Track 组织。**Track A 是纯局部改动、低风险、对下游最直接受益,建议先做;Track B 解决结构性问题;Track C 是架构演进,需独立实验分支验证;Track D 是基建**。每项含:现状 → 方案 → 预期收益 → 风险。

---

### Track A:通知/追踪热路径去分配(P0)

#### A1. trigger 路径的对象分配

**现状**
- `src/notify.ts:261` `{...inputInfo, source, type}`:每次 trigger 浅拷贝一次 info;`:376` 生产模式下的无用解构。
- `src/notify.ts:349-359`:多 dep 时 `effects.push(...dep)` 后 `createDep(effects)` —— 为"稳定化"临时创建一个 **Set**,而 `triggerEffects` 第一行又 `[...dep]` 把它展开回数组,双重拷贝纯属浪费。
- `src/notify.ts:416` `[...dep]`;`:420` 与 `:430` 对 `activeScopes.at(-1)` 的重复读取与重复比较(triggerEffects 判断一次、triggerEffect 又判断一次并抛错)。
- `src/notify.ts:438` `effect.run([info])` 的单元素数组。

**方案**
1. 多 dep 收集改为直接 push 进普通数组(去掉 `createDep(effects)`),`triggerEffects` 接受数组时不再展开。
2. 单 dep 快路径:dep 是 `CompactDep` 且只有 single 订阅者时,直接调用 `triggerEffect`,完全不创建迭代器(配合 A4)。
3. `triggerEffects` 循环外读一次 `activeScopes[activeScopes.length-1]`,循环内只比较;去掉 `triggerEffect` 里的重复检查(改为 `__DEV__` 断言)。
4. info 构造下沉:`trigger`/`triggerPrimitiveAtomValue` 不再预先 spread,把 `(source, type, inputInfo)` 直接传给 effect 的新内部入口 `runFromTrigger(source, type, inputInfo)`,由 **需要 info 的 Computed(manualTracking/patch 模式)** 自己组装 `TriggerInfo` 并入队;普通 effect(axii 的绑定)走零参 `run()`。`run(infos)` 公开签名保留,内部改走新入口。
5. primitive atom 写入路径特化(配合 atom.ts):`triggerPrimitiveAtomValue(target, newValue, oldValue)` 改为标量参数,`src/atom.ts:172` 的 inputInfo 对象仅在存在 patch 型消费者时才构造(见 C3 的 dep 标志位;第一阶段可以先无条件延迟到 `runFromTrigger` 内,已消除无订阅者/轻订阅者场景的全部分配)。

**预期收益**:atom 写路径分配从 6~8 个对象降到 0~1 个;结合 §2.2,minor GC 频率预计降一个数量级;`write atom with 1 subscriber` 预计 1.5~2.5×。
**风险**:中。`run(infos)` 的调用契约在 data0 内部(Computed.run、digestEffectSession、recursiveMarkDirty)与 axii(`LightBindingEffect` 未覆写 run 的传参)间需要梳理;用 `__tests__` + axii 测试套件回归。

#### A2. dispatch 事件系统:无监听者零成本

**现状**:`src/reactiveEffect.ts:134` `dispatch(event, ...args)` rest 数组每次分配;`src/notify.ts:247-250`(track 成功时)与 `:433`(每次触发每个 effect)在**无监听者时也构造 payload 对象**。一次"写→重算"周期至少 5 次 dispatch。

**方案**
1. `dispatch` 改单参签名 `dispatch(event, arg?)`(内部调用点全部只传 0/1 个参数,rest 消除)。
2. payload 构造前先判断 `this._eventToCallbacks?.has(event)`(增加一个只读快查,或位标志缓存"监听过哪些事件"),没有监听者直接 return。`onTrack`/`onTrigger` 等调试钩子行为不变。

**预期收益**:每次写省 5+ 个数组/对象分配;track 热路径(`at() tracked` bench)预计 15~30% 改善。
**风险**:低。`dispatch` 是内部 API,`callbacks.onXxx` 语义不变。

#### A3. Computed 固定开销瘦身:status 惰性 atom、Date.now、cleanPromise 惰性、调度参数惰性

**现状**
- `src/computed.ts:171`:每个 Computed 构造即创建 `status` atom;每次重算写它 3 次,每次写分配 1 个 inputInfo(§3-7)。`updatedAt` 已经做过同款惰性化(`:152-160`),status 还没有。axii 明确因为这个开销绕开了 computed(`LightBindingEffect.ts:37` 注释)。
- `src/computed.ts:425,482`:`new Date().getTime()` → 应为 `Date.now()`。
- `src/computed.ts:391`:非 immediate 每轮 dirty 无条件 `createCleanPromise()`(Promise + 3 闭包),绝大多数 computed 无人 await。
- `src/computed.ts:386`:给 `scheduleRecompute` 传 `[...(this._triggerInfos ?? [])]`,内置调度器不消费。
- `src/computed.ts:216`:async 路径每次重算 `uuid()`(随机字符串),换成自增整数即可。

**方案**
1. status 存普通数字字段 `_statusValue`,内部全部读写字段;`get status()` 首次访问才创建 atom 并保持同步(与 `updatedAt` 同款)。外部 `list.status()` / `.status.raw` 兼容(`__tests__/common.spec.ts:143` 有响应式用法,保留)。
2. `Date.now()` 替换;uuid → 自增 id。
3. cleanPromise 惰性:dirty 时不再预创建;`get cleanPromise()` 按当前状态惰性生成(dirty→pending,clean→resolved),`recompute()` 返回值语义不变。
4. 调度器第三参:仅当 `scheduleRecompute.length >= 3` 时才拷贝传入。

**预期收益**:每 Computed 常驻内存 -150~250B(1083B→~850B);每次重算减 3 次 atom 写 + 3 个对象 + 1 个 Date + (调度路径) 1 个数组 + 1 个 Promise;`chain of 10` 与 `create+destroy computed` 预计 30%+;为 B3(map per-item Computed)直接减负。
**风险**:低-中。status 惰性升级要保证"升级前的写"不丢(创建 atom 时用当前值初始化即可);cleanPromise 惰性需覆盖 async patch 的 await 路径(`__tests__/asyncComputed.spec.ts` 已有覆盖)。

#### A4. CompactDep 迭代快路径

**现状**:`src/dep.ts:87-93` 的 generator 迭代器让最常见的"单订阅者 dep"在每次 trigger 时都创建 generator + 数组(§3-3)。

**方案**:给 Dep 增加 `forEachEffect(fn)` 原型方法(CompactDep:直接调用 single 或遍历 overflow;Set dep:原生 forEach),notifier 全部改用它;或在 CompactDep 上暴露 `single`,notifier 做单订阅者直调。

**预期收益**:配合 A1,axii 轻绑定场景(1 dep 1 effect)每次写零迭代器分配。
**风险**:低,纯内部结构。

#### A5. effect session(batch)数据结构

**现状**:`src/notify.ts:86-88`,session 用 `Set<ReactiveEffect>` + `WeakMap<effect, TriggerInfo[]>`;每次 batch 内 trigger 都是 Set.add + WeakMap.get/set + push;digest(`:107-137`)再逐个 delete。**实测 batch 路径(340ns/写)比非 batch(246ns)还慢**,与 batch 的设计初衷相反。空 session 的 digest 也要走 try/finally + 创建空 Set 迭代器。

**方案**
1. 队列改为普通数组 `effectsInSessionQueue: ReactiveEffect[]`,去重标志与 payload 挂到 effect 实例字段(`_inSession: boolean` / `_sessionInfos`,数字代次 generation 更佳,避免复位写),digest 后按代次自然失效,不需要 WeakMap 哈希与逐个 delete。
2. `digestEffectSession` 增加 `queue.length === 0` 的快出口。
3. 配合 A1,session 内 payload 同样只为 patch 型 Computed 记录。

**预期收益**:`batch write 100 atoms` 预计 ≥2×(340ns→<170ns/写);RxList 每次 splice/set 内部都要建立-销毁一次 session(`sendTriggerInfos`,`src/computed.ts:527-539`),同样受益。
**风险**:低-中。注意 digest 过程中新 trigger 追加入队的语义(现有 while 迭代已支持,数组队列用索引游标等价实现);异常复位逻辑保留。

#### A6. 杂项微优化(合并为一个 PR)

- `activeScopes.at(-1)` → `activeScopes[activeScopes.length-1]`(`notify.ts:161,179,211,420,430`、`reactiveEffect.ts:73` 等十余处;`.at()` 是通用方法调用,热路径上比索引访问慢)。
- `Notifier.instance` static getter 在模块内缓存为常量(track/trigger 每次都过一遍 `_instance ||` 判断)。
- 热路径 `assert(...)` 包进 `if (__DEV__)`(生产 tree-shake 掉;`RxList.map` 的 applyPatch 里每条 triggerInfo 2 次 assert:`RxList.ts:501-502`)。
- `run(infos: TriggerInfo[] = [])` 默认参数在每次无参调用时分配空数组(`recursiveMarkDirty` 循环里 `effect.run()`,`computed.ts:325`)→ 模块级 `EMPTY_INFOS` 常量或允许 undefined。
- `trackTargetFrames.at(-1)?.push(target)`(`notify.ts:173,202`):维护一个 `currentTrackFrame` 引用代替每次数组尾查。
- `finishFullRecompute` 中,`effectsInSession` 为空时跳过 `createEffectSession/digestEffectSession` 的 try/finally 与迭代器创建(依赖 A5 的快出口)。

**预期收益**:单项 1~5%,合计对 track/trigger 路径 10~20%。
**风险**:极低。

---

### Track B:RxList / 集合类型的算法与依赖粒度(P1)

#### B1. 消除 `forEach`/iterator 的冗余 per-index track

**现状**:`src/RxList.ts:382-389`(forEach)与 `:393-409`(iterator)把每个元素的读转发到 `at(i)`,在 tracking 上下文里**为每个 index 建立一个 dep**,同时又 track 了 `ITERATE_KEY`。而所有变更路径(splice → METHOD key=ITERATE_KEY,`notify.ts:301-302`;set → SET case push ITERATE_KEY dep,`notify.ts:322-325`;reorder → METHOD)都必然命中 ITERATE_KEY 订阅者。**per-index track 完全冗余**:一个在 computed 里 forEach 千行列表的用户,会得到 1000 个 index dep(每次重算 initDepMarkers/finalizeDepMarkers 双遍历这 1000 个),并且把列表推入 `indexKeyDeps` 慢路径(见 B2)。

**方案**:forEach/iterator/`toArray` 内部改为只 track `ITERATE_KEY`,元素读取直接走 `this.data[i]`;`at()` 单独调用时保留 per-index 细粒度(这是它的设计价值:`splice` 时只触发受影响 index,`RxList.ts:372` 注释)。

**预期收益**:遍历型 computed 的重算成本从 O(n) 个 dep 降到 O(1);消除"遍历过的列表永久走 splice 慢路径"的入口之一;`at() tracked x100` bench 变化为可对照指标。
**风险**:中。行为变化:此前"forEach 后某个 index 被 set"会精确触发,现在 set 也会触发 ITERATE_KEY(其实现状就触发,语义不变,只是少了冗余触发一次的 index dep);需在 CHANGELOG 标注对 `onTrack` 调试输出的影响。回归依赖 `__tests__/rxList.spec.ts` 全量。

#### B2. 修复 `indexKeyDeps` 性能悬崖 + splice 按订阅触发

**现状**(两个叠加的问题)
1. **悬崖**:`at()` 建立的 index dep 存入 `indexKeyDeps`(`RxList.ts:373-380`)后**永不清理**——effect 全部退订后 dep 变空,但 Map entry 还在,`hasIndexKeyDeps = !!this._indexKeyDeps?.size`(`:163`)永远为 true,此后该列表所有 splice 都走慢路径。代码里也留了 FIXME(`:1149`)。
2. **慢路径本身 O(range)**:`RxList.ts:196-199`,只要 `hasIndexKeyDeps`,splice 就对 `start..changedIndexEnd`(长度变化时直到新末尾)**逐 index trigger SET**,每次 trigger 都有完整的 info 分配 + depsMap 查找,即使只有 1 个 index 被订阅。`oldValues` 记录(`:181-186`)同样 O(range)。

**方案**
1. 清理:`finalizeDepMarkers` / `cleanup` 删除 effect 后,若 dep 变空且 dep 归属 RxList 的 indexKeyDeps(给 dep 加 owner 弱引用或由 RxList 在 splice 前惰性扫描清空项),从 Map 中移除;`hasIndexKeyDeps` 恢复 false 后重新享受 fast path(`:169-175`)。最简实现:splice 慢路径入口先做一次 `indexKeyDeps` 清扫(空 dep 删除),摊销成本 O(订阅数)。
2. 按订阅触发:慢路径不再遍历 `start..changedIndexEnd`,改为遍历 `indexKeyDeps` 的 key(通常远小于 range),只对 `key >= start` 且落在受影响区间的 index trigger,`oldValues` 只为这些 key 记录。
3. `reorder`(`:240-249`)同款处理。

**预期收益**:"曾被 at()/遍历过的长列表"的 splice 从 O(n) 次 trigger 降到 O(被订阅 index 数);对 axii 场景(行内 `at(index)` 订阅稀疏)是数量级改善;同时修掉一个必然发生的内存缓慢增长。
**风险**:中。需要保证"订阅了 index 5,splice(3,1) 后 index 5 的新值触发"语义不变(按订阅遍历天然覆盖);`__tests__/rxList.spec.ts` + axii `rxListHost.spec` 回归。

#### B3. `RxList.map`:纯 mapFn 跳过 per-item Computed

**现状**:`RxList.ts:466-490`(全量)与 `:524-548`(patch)为**每一行**创建 `new Computed(...)` → `run()` → `hasDeps()` 检查 → 多数情况(mapFn 是纯函数)立即 `destroy()`。一行的成本 ≈1.5µs(§2.3 create+destroy bench),其中大头是 Computed 构造(status atom 等,A3 后会显著变小)+ prepare/completeTracking 全套 + destroy。`agentspace/prompt/rxlist_map.md` 也记录过这里的复杂度。

**方案**(保语义的自适应探测)
1. 复用一个**常驻探测 effect**(挂在 map 产物上):对每行,`prepareTracking → mapFn → completeTracking`,看 `deps.length`。无依赖 → 直接使用返回值,清空探测 effect 的 deps(数组 length=0 + dep.delete),**不创建任何对象**;有依赖 → 对该行按现状升级为常驻 per-item Computed(重跑一次该行)。
2. 每行独立判断,天然处理"mapFn 只在部分行读响应式数据"的情况,不改变可观察行为。
3. `options.skipItemEffect` 保留为显式快路径;文档标注推荐。

**预期收益**:纯 mapFn(绝大多数,如 `item => <div>{item}</div>`)的 map 全量/新增行成本降为"一次轻量 track 会话",`create+destroy RxList(100) with map` 预计 ≥2×;长列表首屏与大批量 push 直接受益。
**风险**:中。探测 effect 的复用要处理 mapFn 内部再创建 effect(children 收集,现有 `shouldKeepMapItemEffect` 的判断逻辑照搬);异常路径 try/finally 复位。实施顺序上建议**先落 A3 再压测本项**,若 A3 后 Computed 创建已足够便宜,本项可降级。

#### B4. `RxList.filter` 重构

**现状**:`RxList.ts:871-906`,filter = `map`(每行 1 个 mapContext + onCleanup 闭包 + remove 闭包)+ 每行 1 个**带 context 参数的 computed**。带 context 意味着每次重算都走 `createGetterContext`(`computed.ts:286-296`):1 个 context 对象 + 1 个 onCleanup 闭包 + **2 个 `.bind()`**。另外行匹配变化时 `filtered.data.indexOf(item)` O(n) 查位置。

**方案**
1. 短期:`createGetterContext` 的 context 对象与 bind 结果缓存在实例上复用(`lastValue` 字段每次重算前更新),所有带 context 的 computed 都受益(不止 filter)。
2. 中期:filter 改为与 map 同款的批式 patch 实现——全量阶段用 B3 的探测方式逐行执行 filterFn,只为"有响应式依赖的行"保留 per-item effect;splice patch 直接算新增行,不再依赖 per-item computed 的 `lastValue` 状态机;用 map 产物维护 source index → filtered index 的映射,替代 `indexOf`。

**预期收益**:filter 千行列表的创建从 ~2000 个对象(computed + context + 闭包)降到 O(有依赖的行);每次行内依赖变化少 3~4 个分配。
**风险**:中-高(中期部分)。filter 的顺序语义(当前实现只保证首元素 unshift,其余 push,顺序本来就是近似的,`RxList.ts:885-890`)要在测试中锁定后再动。短期部分风险低,先做。

#### B5. `RxMap`/`RxList`/`RxSet` 派生 meta 的真惰性化

**现状**:注释 `FIXME 目前不能用 cache 的方法在读时才创建`(`RxMap.ts:158`、`RxList.ts:1133`、`RxSet.ts:324`)——原因是惰性创建发生在 autorun/computed 内会被当成 children 收集、cleanup 时误销毁。于是:
- 每个 RxList 无条件带 1 个 length computed(`RxList.ts:1137-1146`);
- 每个 RxMap 无条件带 keys(RxList)+ values/entries(两个 **map 派生**,又各带 length computed)+ size computed(`RxMap.ts:157-208`),空 RxMap 的固定成本 ≈5 个 Computed + 若干 atom,估算 >5KB;
- RxList(empty) 1793B 中 length computed 占约一半。

**方案**:惰性创建时用"脱离当前收集上下文"解决 children 误收集——data0 已有全部原语:创建前 `ReactiveEffect.activeScopes` 顶层 `pauseCollectChild()`(或临时压入 null scope),创建后恢复;axii 的 `detachFromCreationContext` 已验证同款思路可行。length/size/keys/values/entries 全部改为首次访问创建。

**预期收益**:RxList 常驻 -~800B(1793→~1KB);RxMap 减 3~4 个 Computed;`groupBy`(每 group 一个 RxList)、链式 map(每层一个 length)等场景按倍数放大。
**风险**:中。要保证首次访问发生在 tracking 上下文中时 track 正确建立(惰性创建 + 立即 manualTrack,现有 getter 结构不变);`destroy()` 只销毁已创建的 meta。

#### B6. spread 传参的栈溢出与 O(n) 拷贝(正确性 + 性能)

**现状**:`replaceData` → `this.splice(0, len, ...newData)`(`RxList.ts:127`)、`concat` 的 `merged.push(...src.data)`(`:1191`)、`groupBy` 的 `unshift(...group)`(`:984`)等——V8 实参上限约 65k,**10 万行的 computed RxList 全量重算会直接 RangeError**;未爆栈时也是 O(n) 实参拷贝。

**方案**:内部新增 `spliceArray(start, deleteCount, items: T[])`(数组参数版,公开 `splice(...items)` 变为其薄包装);所有内部调用点(replaceData/concat/groupBy/map patch 等)改传数组。顺带消除 trigger info `argv` 里 `[start, deleteCount, ...items]` 的再拷贝(`:171,193`),argv 直接引用 items 数组(patch 消费方 `argv.slice(2)` 的约定同步调整为 `argv[2]` 为数组,内部消费点一起改)。

**预期收益**:修复大列表崩溃;大批量 replace/concat 减一次 O(n) 拷贝。
**风险**:低-中。`argv` 结构调整涉及所有 applyPatch 消费点(map/toSorted/groupBy/indexBy/toMap/toSet/slice/concat/selection,均在 data0 内部)+ axii `RxListHost.handleSplice`(消费 `argv`,需要同步适配或保留兼容形状——**建议 argv 形状不变、只改传参方式**,把 `[start, deleteCount, ...items]` 的构造放到确有 patch 消费者时,与 C3 合并考虑)。

#### B7. 其余集合算法项(低优先级,顺手做)

- `toSorted` patch 删除用 `indexOf` O(n)(`RxList.ts:344`):有序数组应二分定位(比较函数已知)。
- `RxSet` 集合运算全量阶段 `[...base.data].filter(...)`(`RxSet.ts:97,144,181,224`):直接迭代 Set,省一次数组物化。
- `RxSet.has()` 每次调用创建新 computed(`RxSet.ts:81-88`):加 per-value 缓存(WeakMap/Map + 引用计数,或文档标注调用方自行缓存)。
- `RxMap` iterator 每次 `Array.from(data.keys())`(`RxMap.ts:139`):直接持有原生 iterator。
- `util.replace` 对象 key 删除 `filter + includes` O(n²)(`util.ts:232`):用 Set。
- iterator 的 `next()` 每步返回新对象 `{value, done}`(RxList/RxMap):改为 generator 或复用 result 对象(评估下游是否依赖对象独立性后再做)。

---

### Track C:架构级演进(P2,独立实验分支)

> C 类改动收益上限最高,但侵入内核数据结构,必须以"实验分支 + 全量 bench + axii/axle 测试套件"三重验证推进,不与 A/B 混在一个发布里。

#### C1. TriggerInfo 的按需构造(dep 级消费者标志)

**思路**:A1-4 把 info 构造推迟到了 effect 侧;本项再进一步——在 dep 上维护 `patchConsumerCount`(track 时按 effect 是否 `manualTracking` 增减)。trigger 时若 dep 无 patch 消费者,`InputTriggerInfo`(含 `argv`/`methodResult` 等 payload,对 splice 是 O(items) 的)完全不构造。RxList 的显式方法(splice/set/reorder)当前无条件组装完整 info(`RxList.ts:171,193,197`),在"只有轻量绑定订阅"的 axii 场景全是浪费。

**预期收益**:axii 直接消费 RxList(无派生结构)时,splice 的 info 成本归零。
**风险**:中。计数正确性(effect 销毁/重track)需要仔细的引用计数测试。

#### C2. 版本计数依赖追踪(替代 w/n 位标记 + 双遍历)

**现状**:每次重算 `initDepMarkers`(全 deps 置位)→ track 时位检查 → `finalizeDepMarkers`(全 deps 再遍历、删除失效项)(`dep.ts:102-127`),deps 多时(遍历型 computed)是 2n 次额外遍历;`maxMarkerBits=30` 的嵌套限制;dep 去重靠 Set 哈希。

**方案**:迁移到 Vue 3.5 / alien-signals 的方案——effect 与 dep 之间用双向链表 link 节点 + 全局版本号:重算前 effect 版本 +1,track 时 link.version = 当前版,重算后一次遍历摘除旧版 link;dep→effect 与 effect→dep 都是 O(1) 链接,无 Set、无位深限制。data0 特有约束需要保留:`manualTracking` 的 patch computed(不走 marker,`useDepMarker=false`)、async effect 的延迟 track(`asyncTracks`)、`trackClassInstance`。

**预期收益**:重算固定开销中的 2n 遍历消除;dep add/remove 从 Set 哈希变链表 O(1);预计对 `read 10 atoms inside recompute`、遍历型 computed 再取 20~40%;内存上 link 节点 vs Set entry 大致持平。
**风险**:高(内核心脏手术)。以独立分支实施,bench + 全测试矩阵(含 axii/axle)通过后再合入;保留旧实现一个 release 周期的回退开关不现实(数据结构互斥),以充分的回归覆盖代替。

#### C3. 调度语义:lazy pull 与菱形一致性(仅评估,暂不实施)

**现状**:sync computed 默认 `immediate = true`(`computed.ts:198`),每次依赖变化立即全量传播。菱形依赖(a→b→d,a→c→d)非 batch 写入时 d 会计算两次,且第一次读到 b 新值 + c 旧值(glitch)。深链每层 ~1.4µs(§2.3)。业界(Vue/preact-signals/Reactively)的解法是 push dirty + pull 重算 + 拓扑序。

**评估结论**:data0 的 patch 体系依赖"变更即时传递 triggerInfos",全面转 lazy pull 会动摇增量计算的时序假设,**不建议作为本轮目标**。保留两个低风险动作:
1. 新增 opt-in 的 lazy computed(读时重算,适合"高频写低频读"的派生值),不改默认。
2. 在 bench 中加入菱形/深链用例,量化 glitch 重复计算的真实代价,为下一轮决策积累数据(见 D1)。

---

### Track D:基建与防回归

#### D1. bench 矩阵扩充

现有 `bench/core.bench.ts` + `scripts/measure-speed.mjs` + `scripts/measure-retained.mjs` 基础不错,补齐:
- 深链(50 层)/宽扇出(1 atom→1000 effects)/菱形传播;
- `forEach`/iterator 在 computed 内遍历 1k/10k 行 + 随后 splice(B1/B2 的直接验证);
- filter/groupBy/createSelection 的创建与 patch;
- 大列表 `replaceData`(10 万行,B6 验证,当前会崩);
- **分配率指标**:`--trace-gc` 的 Scavenge 计数(见 §2.2)或 `performance.measureUserAgentSpecificMemory`,纳入 measure-speed 输出;
- axii 侧端到端:现有 `tests/bench`(50000 行渲染)在 A/B 完成后跑对照。

#### D2. CI 回归门禁

- `pnpm bench` 结果落 JSON(vitest bench `--outputJson`),PR 上与 main 基线对比,热路径用例回退 >10% 报警;
- `measure-retained.mjs` 的字节数同样入库对比;
- 每个 Track 的 PR 必须附带前后对比数据。

#### D3. 构建目标核查(小项)

dist 已验证干净(class field 无 defineProperty 转译,`__DEV__` 分支正确剔除)。可选:build target 升 es2022(原生 class fields),预计收益边际,验证后决定;保持 `assert` 改造(A6)后再次确认 tree-shaking。

---

## 5. 实施顺序与依赖关系

```
阶段 1(并行,互不依赖):
  A2 dispatch 零成本      A3 Computed 瘦身      A6 杂项      D1 bench 扩充
阶段 2:
  A1 trigger 去分配(依赖 A2/A4 的接口)   A4 CompactDep   A5 session 重构
  → 阶段 2 结束:重跑全部基线,预期达成"atom 写 ≥2x、GC 次数降 10x"
阶段 3:
  B1 forEach track 粒度 → B2 indexKeyDeps(B2 依赖 B1 减少慢路径入口)
  B6 spread 修复(含正确性,可提前) B5 meta 惰性化
阶段 4:
  B3 map 探测(先用 A3 后的数据重新评估) B4 filter B7 杂项
阶段 5(独立分支,不阻塞发布):
  C1 → C2;C3 仅产出评估报告
```

每阶段一个(或数个)独立 PR,每个 PR:改动 + bench 前后数据 + `__tests__` 全绿 + axii/axle 测试套件(工作区内 `/agent/repos/axii`、`/agent/repos/axle` 可直接联调)全绿。

## 6. 风险与兼容性约束

1. **公开 API 不破坏**:`atom/computed/RxList/RxMap/RxSet/autorun/once/batch/ManualCleanup/ReactiveEffect` 的签名与可观察语义不变。`run(infos)` 等内部协议变化需确认 axii(`LightBindingEffect`/`RxListHost` 直接消费 `TriggerInfo.argv` 等)与 axle(`BindingEffect`)同步验证。
2. **triggerInfo 形状是事实上的对外协议**(axii `RxListHost.handleSplice` 消费 `argv`/`methodResult`):C1/B6 中任何 payload 结构调整都以"形状不变、构造时机延后"为原则。
3. **调试钩子**(`onTrack`/`onTrigger`/`onDirty`/retainedDiagnostics)在有监听者时行为不变;B1 会改变 `onTrack` 观察到的 dep 粒度,CHANGELOG 说明。
4. **时序语义**:immediate computed 的同步重算时序、patch 的 triggerInfos 顺序、effect session 的去重语义均不变;A5 仅换数据结构。
5. 版本策略:A/B 合入走 minor(2.3.0);C2 若实施,走 major 或长周期 beta。

## 7. 验收标准(量化)

| 指标 | 基线 | 阶段 2 后目标 | 阶段 4 后目标 |
|---|---|---|---|
| atom 写,1 订阅者 | 246ns | ≤120ns | ≤100ns |
| 2M 次写的 minor GC 次数 | 1309 | ≤150 | ≤150 |
| batch 写 100 atoms/轮 | 34µs | ≤15µs | ≤15µs |
| chain of 10 传播 | 13.8µs | ≤8µs | ≤7µs |
| create+destroy RxList(100)+map | 157µs | — | ≤60µs |
| at() tracked ×100 | 18.2µs | ≤12µs | ≤10µs |
| computed 常驻内存 | 1083B | ≤850B | ≤850B |
| RxList(empty) 常驻内存 | 1793B | — | ≤1100B |
| 10 万行 replaceData | RangeError 崩溃 | — | 正常,≤50ms |
| `__tests__` + axii + axle 测试 | 全绿 | 全绿 | 全绿 |

以上目标为工程估算,若某项实测证伪(收益不及预期或风险过高),在对应 PR 中记录数据并调整,不硬凑指标。
