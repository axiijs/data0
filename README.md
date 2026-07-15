# data0

响应式状态管理库,为 [axii](https://github.com/axiijs/axii) / axle 等增量渲染框架提供底层数据结构。核心特点:

- **无 Proxy 的显式响应性**:`atom` / `computed` / `RxList` / `RxMap` / `RxSet`,变更必须经由实例方法,方法即变更边界。
- **增量派生**:`map` / `filter` / `groupBy` / `toSorted` / `slice` 等派生结构默认按 `triggerInfo` 增量维护,不做全量 diff。
- **面向框架的触发协议**:`triggerInfo`(method/argv/methodResult/reorderInfo)将结构化变更透传给下游,渲染层可据此做最小 DOM 操作。

> 本 README 是 data0 的**语义契约**:它定义承诺面(什么行为受保证、什么属于契约外)。review 与缺陷报告以本文与 [AGENTS.md](./AGENTS.md) 为裁决依据。

## 安装与快速上手

```bash
npm install data0
```

```ts
import {atom, computed, RxList, autorun, batch} from 'data0'

// 原子值:函数式读写
const count = atom(0)
count()          // 读(在 computed/autorun 内会建立依赖)
count(1)         // 写(同步触发订阅者)
count.raw        // 读但不追踪

// 派生值:默认急切重算
const double = computed(() => count() * 2)

// 响应式列表与增量派生
const list = new RxList([1, 2, 3])
const doubled = list.map(x => x * 2)      // 增量维护
const evens  = list.filter(x => x % 2 === 0)
list.push(4)                               // doubled/evens 增量更新

// 副作用
const stop = autorun(() => console.log(double()))

// 批量写:订阅者在 batch 结束时统一执行一次
batch(() => { count(2); count(3) })

stop()
doubled.destroy(); evens.destroy(); list.destroy()
```

## 语义契约

### 1. 变更边界与所有权(零拷贝采纳)

- 响应性完全建立在"**通过实例方法修改**"之上。`RxList.splice/set/push/...`、`RxMap.set/delete/...`、`RxSet.add/delete/replace` 内部负责触发;没有 Proxy 兜底。
- **构造与 `RxSet.replace` 直接采纳传入容器的引用**(`new RxList(arr)` 后 `list.data === arr`),视为所有权移交。之后绕过方法直改原容器(`arr.push(x)`)不会触发任何通知,派生结构静默失联——这是契约内行为,不是缺陷(详见 AGENTS.md A3)。
- `data`(以及 atom 的 `.raw`)是**只读视图**:可以读,不可以写。

### 2. 传播模型(急切推,同步)

- atom 写入后**同步**执行订阅者,顺序为订阅顺序;computed 默认立即重算(`immediate`),async getter 默认经 microtask 调度。
- **对象 atom 的浅属性写入会触发**:`obj.x = 1` 经 proxy set 陷阱通知订阅者;`obj.raw.nested.n = 1` 或取出嵌套对象后再改**不会**触发,需 `obj({...})` 整替换(无深 Proxy)。
- **atom 的对象特性由创建时初始值的形态决定,不随后续写入迁移**:以原始值/`null`/`undefined` 起手的 atom 是无 Proxy 的轻量形态,之后写入对象(`a(user)`)后整值读写与依赖追踪一切正常,但**属性级读写不可用**——`a.x` 读不到值对象的属性、`a.x = 1` 既不写入值对象也不触发(落在 atom 函数对象自身上)。需要属性级用法时以对象初值创建 atom;以 `null` 起手的"暂无数据"atom 请坚持整值替换。class 实例 atom 的属性**写**会写穿实例并触发,属性**读**不转发(属性读仅对 plain object 承诺)。特征测试见 `__tests__/deepReview2026H3Round3.spec.ts`。
- **菱形依赖存在 glitch**(AGENTS.md A1):`a→c` 且 `a→b→c` 时,`c` 可能先以"新 a + 旧 b"重算一次,下游可观察到中间值并产生重复重算;**终值保证收敛正确**。对中间态敏感的副作用应自行防抖或读 `.raw` 终值。
- **同步重算环会抛错**:在同步 computed 重算过程中又触发它自身的依赖变更,会抛出 `detect recompute triggerred in sync recompute`,请将变更移到调度回调中。

### 3. `batch()` 语义

- batch 内的写入立即生效于**数据本身**(atom 的 `.raw`、`RxList.data`),但订阅者(含 computed 的重算与标脏)推迟到 batch 退出时统一执行。
- 因此 **batch 内"先写依赖、再读该依赖的 computed"读到的是进入 batch 前的旧值**(AGENTS.md A2),batch 退出后恢复一致。需要读写一致时在 batch 外读,或使用 `autorun`(见下;其默认调度下重跑在 microtask,同步场景请传 `true`)。
- **batch 退出后,所有派生结构必须等于从终态 source 全量重算的结果**(A1/A2 的"仍属缺陷"边界)。一次 digest 重放多条变更(batch 多操作、自定义延迟调度器积累)时,部分算子会自动回退全量重算以保证该不变量(见支持矩阵脚注),下游只应依赖结果一致性,不应依赖"必然增量"。
- batch 中某个订阅者抛错不会阻断其余订阅者;第一个错误在 digest 完成后抛给 batch 调用方。

### 3.1 `autorun` 调度

- 默认 `autorun(fn)` 的重跑经 **microtask**(`Promise.resolve().then`)调度,首次执行仍同步。
- 需要与写入同步一致时使用 `autorun(fn, true)`(立即重跑)。

### 4. RxList 参数契约

- `splice(start, deleteCount, ...items)` 的参数按 `Array.prototype.splice` 规范归一化(ToIntegerOrInfinity:NaN→0、小数截断、负数从尾部回退、越界 clamp、`-0`→`+0`)。
- **`toSorted(compare)` 的 comparator 必须对元素值域构成一致全序**(与 `Array#sort` 的 consistent-comparator 要求相同)。`NaN` 元素 × 裸数值 comparator(`(a,b)=>a-b` 返回 NaN)违反一致性,属契约外;需要含 `NaN` 的列表请用 NaN 归一化的 comparator。同理,列表可能**驻留** `undefined` 元素(含稀疏洞被 reorder 物化)时,comparator 必须与 `Array#sort` 的强制规则一致地把 `undefined` 排到末尾(引擎从不为 undefined 调用 comparator、一律排尾;把 undefined 排前的 comparator 与全量语义必然分叉,属契约外)——**变更本身**涉及 undefined 时仍自动回退全量,无须 comparator 处理。元素身份(增量删除定位)按 `Object.is`(`NaN` 可定位,`0`/`-0` 可区分);等值 tie 组内存在可区分成员时增量删除自动回退全量重算。
- **`triggerInfo.argv` 透传用户原始参数**(不归一化)。这是 axii/axle 依赖的协议;消费 argv 的派生结构必须自行归一化(内部实现均已如此)。
- **`EXPLICIT_KEY_CHANGE` 必须消费 `info.newValue`(以及需要时的 `methodResult` 旧值),不要回读 `source.data[key]`**。batch/延迟调度下一次 digest 可能同时含 EKC 与结构操作;重放时 `source.data` 已是终态,按终态下标读出会与操作时位置错位,导致宿主镜像与源分叉。data0 自身的 map/filter 等增量路径已按协议字段消费。
- `set(index, value)` 的契约是**替换已存在的稠密行**。越界/负数/非整数/≥ 2^32-1 的 key 属于契约外用法:行为等同普通数组赋值(可能产生稀疏数组、`length` computed 不更新;≥ 2^32-1 的正整数不是数组下标,`length` 完全不变),key 原样透传给下游。若列表已有 `atomIndexes`(`map` 使用了 index 参数),越界 set 会为写入位分配 index atom,派生 `map(index)` 不再因此崩溃;稀疏洞位仍按数组语义保留。
- `at(index)` 支持负索引;对具体 index 的读取建立细粒度依赖,收缩(如 `pop`)会通知被裁剪的 index。

### 5. async 契约

- **async getter 只追踪第一个 `await` 之前读取的依赖**;需要跨 await 追踪时使用 generator getter(逐段追踪)。
- **async applyPatch 同样只在同步段建立追踪**;挂起期间到达的源变更会排队,由后续 patch 轮次消化,不丢失。
- **destroy 取消在途 async patch**:destroy 后已挂起的 applyPatch 恢复执行时,其结果不再被应用,对已销毁实例的写入是 no-op(见 §6)。
- async 错误经 `cleanPromise` reject 与 `error` 事件派发;两者都无人监听时 `console.error` 兜底,不产生 unhandled rejection。

### 6. 生命周期

- **谁创建,谁销毁**:派生结构不随 source 自动销毁,需调用 `.destroy()`(或 `destroyComputed`)。派生结构销毁时负责清理自己创建的行级 effect 与惰性 meta(`length`/`keys`/`size` 等)。
- **destroy 对源模式与计算模式一视同仁**:`new RxList(arr)` 这类无 getter 的源结构同样派发 `destroy` 事件、清理 children 与惰性 meta。destroy 幂等,重复调用安全。
- **destroy 后结构只读**:已销毁实例的变更方法(`splice`/`set`/`add`/`replace` 等)一律 no-op(dev 构建打印警告),数据保持销毁当刻的快照;destroy 也会取消在途 async patch 的后续应用。销毁的派生结构从此不再接收 source 的任何更新。
- 在 `autorun`/`computed` getter 内创建的响应式对象会被收集为 child,随宿主重算/销毁自动清理(包括其 `context.onCleanup` 注册的清理与惰性 meta);不希望被收集时用 `ReactiveEffect.createDetached`。
- `map` 的 `context.onCleanup` 注册行级清理,随行移动,行删除/替换/整体销毁时各执行一次;`map` 回退全量重算时,旧行的 cleanup 同样各执行一次。

## 派生结构 × 源操作支持矩阵

图例:**增量** = 增量维护;**重算** = 正确但回退全量重算;**无关** = 该操作不影响结果,自动忽略。

| 派生结构 | splice(增删) | set(替换) | reorder(sortSelf/reposition/swap) |
|---|---|---|---|
| `RxList.map` | 增量 | 增量 | 增量 |
| `RxList.filter` | 增量 | 增量 | 重建(按 indicator 顺序) |
| `RxList.toSorted` | 增量(等值 tie/含 undefined/批量超阈值→重算) | 增量(等值 tie/含 undefined→重算) | 重算(tie 稳定序随源序) |
| `RxList.slice` | 增量(负边界→重算) | 增量 | 重算 |
| `RxList.concat` | 增量(批量多条→重算) | 增量 | 重算 |
| `RxList.groupBy` | 增量 | 增量 | 组内重排 |
| `RxList.indexBy` / `toMap` / `toSet` | 增量 | 增量 | 无关 |
| `RxList.reduce` / `reduceToAtom` | 尾部追加增量,其余重算 | 重算 | 重算 |
| `RxList.find` / `findIndex` / `some` / `every` | 增量(响应式谓词→重算) | 增量 | 重算 |
| `createSelection` / `createSelections` | 增量 | 增量 | 增量 |
| `createIndexKeySelection` | 增量 + 指示器按 index 校正 | 增量 | 增量 + 指示器校正 |
| `RxList.length`、`RxMap.keys/values/entries/size`、`RxSet.size` | 增量(`clear`/`replace` 部分重算) | 增量 | 无关 |
| `RxSet.difference/intersection/symmetricDifference/union/toList` | add/delete/replace 均增量(`replace` 的 `newItems` 按 Set 语义去重) | — | — |

**多变更重放脚注**:矩阵格子描述"一次 digest 恰一条变更"的行为。一次 digest 积累多条变更(batch 多操作、自定义延迟调度器)时,triggerInfo 的操作时位置与重放时的终态 source 可能不一致。`groupBy`、`slice`、`findIndex`(及其派生 find/some/every)、`reduce`/`reduceToAtom`(纯尾插序列,判定与 index 都按操作时长度)经 **digest 重放内核**(`src/digestReplay.ts`,从终态逆向还原每条变更操作时的源状态快照)在多变更下保持增量;快照不可重建时自动回退全量重算——触发条件:`set` 的旧值为 `undefined`(合法 undefined 元素与越界扩长在协议内不可区分)、非稠密下标的 `set`、未知方法。`map` 在行使用 index atom(`mapFn(item, index)`)或行含响应式依赖时多变更回退;`concat` 多变更回退(多源 offset 依赖各源操作时长度),越界 `set` 使源段长度跳变时单变更也回退(防跨段错位),同一源占多个操作数位置(如 `a.concat(a)`)时恒回退(一条变更对应多个段,按段位置的增量无法同时表达);`toSorted` 在插入元素与既有元素等值(tie)、变更涉及 `undefined` 元素值、增量删除的 tie 组内存在 `Object.is` 可区分成员(如 `0` 与 `-0`)、或**单次批量变更超过阈值**(插入数+删除数 > 64 且「> 派生长度/4 或 > 4096」——排序列表的批量逐项增量是 O(k×m),超过实测交叉点后全量重算更快且下游代价相同)时回退。回退是正确性措施,结果不变,只损失该次增量性。`groupBy` 的批量 splice 走单遍分桶增量(每组至多一次删除 + 一次插入 splice,幸存组引用稳定),不回退。

矩阵行为由固定 seed 的差分 fuzz(`__tests__/broadOperatorsFuzz.spec.ts` 覆盖 map/filter/toSorted/slice/concat/toSet/groupBy/findIndex/length/RxSet 运算(含 toList)/RxMap 派生,`__tests__/duplicateValuesFuzz.spec.ts` 覆盖重复值域,`__tests__/batchReplayFuzz.spec.ts` 覆盖 batch 多操作重放与 toSorted 等值 tie,`__tests__/deepReview2026H2Findings.spec.ts` 覆盖 selection 家族的重复 item 域与 toSorted 的 undefined 元素值域)与各专项 spec 共同钉住。**新增派生结构或新增源操作时,必须同步补全本矩阵与对应差分测试;矩阵中声明"增量"的格子必须有差分验证。**

## 架构语义(不作为缺陷)

以下行为经深度 review 评估后确认为架构绑定的既定语义,**不修**,详见 [AGENTS.md「架构决策与已知语义边界」](./AGENTS.md):

- **A1** 菱形依赖 glitch(中间值可观察、重复重算,终值收敛);
- **A2** `batch()` 内读 computed 得旧值;
- **A3** 构造/`replace` 零拷贝采纳外部容器引用。

其可执行定义在 `__tests__/architectureSemantics.spec.ts`。

## 开发与验证

```bash
pnpm install --frozen-lockfile
pnpm test --run        # 全量测试(含差分 fuzz、交错枚举、不变量自检)
pnpm type-check
pnpm build
pnpm exec vitest run --coverage
pnpm bench             # 性能基准(热路径改动必须跑)
```

- CI(`.github/workflows/ci.yml`)在每个 push/PR 上执行以上基线。
- dev 构建(`__DEV__`)内置全局不变量断言(作用域栈/追踪栈平衡、行级记账对齐),违约立即抛错;生产构建零开销。
- 贡献与 review 纪律见 [AGENTS.md](./AGENTS.md) 与 [CONTRIBUTING.md](./CONTRIBUTING.md)。

## License

[MIT](./LICENSE)
