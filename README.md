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
- **构造与 `RxSet.replace` 直接采纳传入容器的引用**(`new RxList(arr)` 后 `list.data === arr`),视为所有权移交。之后绕过方法直改原容器(`arr.push(x)`)不会触发任何通知,派生结构静默失联——这是契约内行为,不是缺陷(详见 AGENTS.md A3)。同名方法的采纳语义并不一致:`RxList.replaceData` 与 `RxMap.replace` **就地更新既有容器**(splice/逐 key 增删,不采纳入参),只有构造与 `RxSet.replace` 采纳引用;`toArray` 同理分叉——`RxList.toArray()` 返回内部数组(只读视图),`RxSet.toArray()` 返回快照副本(特征钉扎见 `__tests__/deepReview2026H3Round8.spec.ts`)。
- `data`(以及 atom 的 `.raw`)是**只读视图**:可以读,不可以写。

### 2. 传播模型(急切推,同步)

- atom 写入后**同步**执行订阅者,顺序为订阅顺序;computed 默认立即重算(`immediate`),async getter 默认经 microtask 调度。
- **对象 atom 的浅属性写入会触发**:`obj.x = 1` 经 proxy set 陷阱通知订阅者;`obj.raw.nested.n = 1` 或取出嵌套对象后再改**不会**触发,需 `obj({...})` 整替换(无深 Proxy)。属性**读**转发仅覆盖 get:`in`/`Object.keys`/spread 不转发到值对象;值属性名与元 API 同名(`raw`/`call`)时读取被元 API 遮蔽(写入仍落值对象)。需要自省/同名属性请读 `.raw` 后操作原对象。
- **atom 的对象特性由创建时初始值的形态决定,不随后续写入迁移**:以原始值/`null`/`undefined` 起手的 atom 是无 Proxy 的轻量形态,之后写入对象(`a(user)`)后整值读写与依赖追踪一切正常,但**属性级读写不可用**——`a.x` 读不到值对象的属性、`a.x = 1` 既不写入值对象也不触发(落在 atom 函数对象自身上)。需要属性级用法时以对象初值创建 atom;以 `null` 起手的"暂无数据"atom 请坚持整值替换。class 实例 atom 的属性**写**会写穿实例并触发,属性**读**不转发(属性读仅对 plain object 承诺)。特征测试见 `__tests__/deepReview2026H3Round3.spec.ts`。
- **菱形依赖存在 glitch**(AGENTS.md A1):`a→c` 且 `a→b→c` 时,`c` 可能先以"新 a + 旧 b"重算一次,下游可观察到中间值并产生重复重算;**终值保证收敛正确**。对中间态敏感的副作用应自行防抖或读 `.raw` 终值。
- **订阅者抛错不会阻断同一次派发中的其余订阅者(batch 与非 batch 一致)**:非 batch 的内联派发同样先执行完全部订阅者,第一个错误在派发完成后抛给写入方,其余错误 `console.error` 上报——被跳过的订阅者不会执行也不会标脏,而 Object.is 判等门会拦截同值重写,"再写一次"救不回静默陈旧的兄弟,因此隔离是跨通道承诺。
- **同步订阅者重入写同一 atom(平衡回写)受支持,但 info 到达序只对"先订阅"的消费者保持因果序**:值与判等立即生效、订阅者不会因自己的写重入自身;而非 batch 的内联派发对**后订阅**(晚于重写者)的 info 消费者交付的是嵌套优先的非因果序(先收到重写、后收到原始写)。库内按 delta 消费 atom info 的结构(selection 家族)按 `currentValues` 终态对账,与序无关;自定义按 delta 消费 atom info 的 applyPatch/onChange 结构应同样以 `.raw` 终态对账,或把重入写放进 `batch`(session 队列保持因果序)。特征钉扎见 `__tests__/deepReview2026H3Round9.spec.ts`。
- **同步重算环会抛错**:在同步 computed 重算过程中又触发它自身的依赖变更,会抛出 `detect recompute triggerred in sync recompute`,请将变更移到调度回调中(深度 ≥2 的同步重写链——重写者触发另一个仍在运行中的重写者——同样命中该断言)。

### 3. `batch()` 语义

- batch 内的写入立即生效于**数据本身**(atom 的 `.raw`、`RxList.data`),但订阅者(含 computed 的重算与标脏)推迟到 batch 退出时统一执行。
- 因此 **batch 内"先写依赖、再读该依赖的 computed"读到的是进入 batch 前的旧值**(AGENTS.md A2),batch 退出后恢复一致。需要读写一致时在 batch 外读,或使用 `autorun`(见下;其默认调度下重跑在 microtask,同步场景请传 `true`)。
- **batch 退出后,所有派生结构必须等于从终态 source 全量重算的结果**(A1/A2 的"仍属缺陷"边界)。一次 digest 重放多条变更(batch 多操作、自定义延迟调度器积累)时,部分算子会自动回退全量重算以保证该不变量(见支持矩阵脚注),下游只应依赖结果一致性,不应依赖"必然增量"。
- batch 中某个订阅者抛错不会阻断其余订阅者;第一个错误在 digest 完成后抛给 batch 调用方。**batch 体自身抛错时体异常优先**:digest 仍照常执行(排队的订阅者不受牵连),期间的订阅者错误降级为 `console.error` 上报,调用方收到的是自己代码的原始异常(不会被订阅者错误静默替换)。

### 3.1 `autorun` 调度

- 默认 `autorun(fn)` 的重跑经 **microtask**(`Promise.resolve().then`)调度,首次执行仍同步。
- 需要与写入同步一致时使用 `autorun(fn, true)`(立即重跑)。

### 3.2 `skipIndicator`(computed 的跳过门)

- `computed()` 的第 5 参与 `RxMap` 构造器的第 5 参接受 `skipIndicator`(`{skip: boolean}`):`skip === true` 期间该 computed 对一切触发**完全静默**——不重算、不标脏、不入队 info、不派发 `dirty`、不调用调度器。
- **skip 期间的变更不会造成增量分叉**:patch 型 computed 在 skip 窗口内丢弃过 info 后自动回退全量重算阶段,解除 skip 后的**下一次触发**(任何来源)全量重算追平终态;解除 skip 本身不触发重算(`skipIndicator` 是普通对象,库不观察它的翻转),在下一次触发到来前读到的是 skip 期间的旧值。
- skip 拦截的是**触发派发**;显式 `recompute(computed, true)` 不受拦截,是 skip 期间强制同步的出口。特征测试见 `__tests__/deepReview2026H3Round9.spec.ts`。

### 4. RxList 参数契约

- `splice(start, deleteCount, ...items)` 的参数按 `Array.prototype.splice` 规范归一化(ToIntegerOrInfinity:NaN→0、小数截断、负数从尾部回退、越界 clamp、`-0`→`+0`)。
- **`toSorted(compare)` 的 comparator 必须对元素值域构成一致全序**(与 `Array#sort` 的 consistent-comparator 要求相同)。`NaN` 元素 × 裸数值 comparator(`(a,b)=>a-b` 返回 NaN)违反一致性,属契约外;需要含 `NaN` 的列表请用 NaN 归一化的 comparator。同理,列表可能**驻留** `undefined` 元素(含稀疏洞被 reorder 物化)时,comparator 必须与 `Array#sort` 的强制规则一致地把 `undefined` 排到末尾(引擎从不为 undefined 调用 comparator、一律排尾;把 undefined 排前的 comparator 与全量语义必然分叉,属契约外)——**变更本身**涉及 undefined 时仍自动回退全量,无须 comparator 处理。元素身份(增量删除定位)按 `Object.is`(`NaN` 可定位,`0`/`-0` 可区分);等值 tie 组内存在可区分成员时增量删除自动回退全量重算。
- **`triggerInfo.argv` 透传用户原始参数**(不归一化)。这是 axii/axle 依赖的协议;消费 argv 的派生结构必须自行归一化(内部实现均已如此)。
- **变更方法返回的数组归调用方所有**(与原生 `Array#splice` 预期一致):`splice`/`clear` 的删除项、`RxSet.replace` 的 `[newItems, deletedItems]` 返回后可自由改写——协议载荷持有独立副本,batch/async applyPatch 等延迟消费不受影响(`reorder` 传入的 order 数组**连同其中的 `[from, to]` 对**同理,调用后仍归调用方,可复用/改写——协议载荷深拷到 Order 对一层)。观察出口(`onChange` 的 handler、自定义调度器的 infos 参数)收到的也是载荷副本,可自由处置——副本深度按协议形状对齐:`reorder` 的 Order 对与 `reorderInfo` 一并独立(改写自己的副本不会毒化兄弟订阅者),`splice` 删除项/插入项、`RxMap.set` 的旧值等**用户值保持引用身份**(按身份记账的观察方可直接匹配)。**直接实现 `applyPatch` 的协议消费者拿到的是共享广播,`triggerInfo.argv`/`methodResult` 只读**(逐 patch 拷贝会落在热路径上,该边界靠本契约约束)。
- **谓词回调按真值语义消费**(与 `Array#filter` 平台惯例一致):`filter`/`find`/`findIndex`/`some`/`every` 的回调返回 number/string 等 truthy/falsy 非布尔值是契约内用法——TS 签名的 `boolean` 只是类型层约定,运行时按真值处理(`filter` 在存储点布尔化;返回值形态族差分见 `__tests__/deepReview2026H3Round8.spec.ts`)。
- **`reorder` 的 pairs 必须构成子集置换**(from/to 集合相等、各自无重复、均为界内整数);`swap` 的两个区间不得重叠(重叠语义自相矛盾,两种构建都拒绝并抛错)。dev 构建对公开 `reorder` 校验置换性并抛错,prod 构建不校验(契约外输入的行为未定义);`sortSelf`/`reposition`/`swap` 内部生成的 pairs 构造性合法,不付校验成本。
- **`groupBy`/`indexBy` 的 getKey、`toSorted` 的 comparator、`reduce`/`reduceToAtom` 的 reduceFn 必须是纯的确定函数**:它们在追踪暂停下执行,**读取响应式数据不建立依赖**——数据变化不会触发重分组/重排/重算(静默陈旧),且 patch 时与建组时返回不一致会破坏增量记账。需要按响应式字段分组/排序时,先用 `map` 把它物化成普通字段(`list.map(t => ({...t, st: t.status()})).groupBy(x => x.st)`);`find`/`findIndex`/`some`/`every` 与 `map`/`filter` 支持响应式回调(见支持矩阵)。同一 item 的 getKey 必须返回一致的键(按 SameValueZero 判等;每次返回新对象的"不稳定键"会使增量与全量重算分叉)。dev 构建对首个元素做一次探测,违反两条契约都会告警。
- **`EXPLICIT_KEY_CHANGE` 必须消费 `info.newValue`(以及需要时的 `methodResult` 旧值),不要回读 `source.data[key]`**。batch/延迟调度下一次 digest 可能同时含 EKC 与结构操作;重放时 `source.data` 已是终态,按终态下标读出会与操作时位置错位,导致宿主镜像与源分叉。data0 自身的 map/filter 等增量路径已按协议字段消费。
- `set(index, value)` 的契约是**替换已存在的稠密行**。**规范下标字符串会归一化为 number**(平台规范:`data["2"] = v` 就是写第 2 行——`set("2", v)`/`at("2")` 与 `set(2, v)`/`at(2)` 完全等价,dev 构建告警提示改传 number);越界/负数/非整数/非规范字符串("02"/"2.5")/≥ 2^32-1 的 key 属于契约外用法:行为等同普通数组赋值(可能产生稀疏数组、`length` computed 不更新;≥ 2^32-1 的正整数不是数组下标,`length` 完全不变),key 原样透传给下游。若列表已有 `atomIndexes`(`map` 使用了 index 参数),越界 set 会为写入位分配 index atom,派生 `map(index)` 不再因此崩溃;稀疏洞位仍按数组语义保留。
- `at(index)` 支持负索引;对具体 index 的读取建立细粒度依赖,收缩(如 `pop`)会通知被裁剪的 index。

### 5. async 契约

- **async getter 只追踪第一个 `await` 之前读取的依赖**;需要跨 await 追踪时使用 generator getter(逐段追踪)。
- **async generator getter/applyPatch(`async function*`)不支持,构造时报错**(否则会被静默当作同步 getter,computed 的值变成一个从未被推进的 AsyncGenerator 对象)。异步计算用 async getter,跨 await 追踪用同步 generator getter。
- async/generator 形态检测基于原生构造器(`AsyncFunction`/`GeneratorFunction`):**构建工具把 async 降级转译(target < ES2017)后无法检测**,会被当作"返回 Promise 的同步 getter"。data0 面向现代 ES 目标,请勿把 getter 转译到 ES2017 以下。
- **async applyPatch 同样只在同步段建立追踪**;挂起期间到达的源变更会排队,由后续 patch 轮次消化,不丢失。
- **destroy 取消在途 async patch**:destroy 后已挂起的 applyPatch 恢复执行时,其结果不再被应用,对已销毁实例的写入是 no-op(见 §6)。
- async 错误经 `cleanPromise` reject 与 `error` 事件派发;两者都无人监听时 `console.error` 兜底,不产生 unhandled rejection。**async 收尾阶段(向订阅者派发/回退全量重算)发生的错误同样 `console.error` 兜底**——该错误属于下游订阅者或回退重算,本 computed 的数据与状态已完成写入,`cleanPromise` 照常 settle(await 方不会因下游错误而挂起)。

### 6. 生命周期

- **谁创建,谁销毁**:派生结构不随 source 自动销毁,需调用 `.destroy()`(或 `destroyComputed`)。派生结构销毁时负责清理自己创建的行级 effect 与惰性 meta(`length`/`keys`/`size` 等)。
- **destroy 对源模式与计算模式一视同仁**:`new RxList(arr)` 这类无 getter 的源结构同样派发 `destroy` 事件、清理 children 与惰性 meta。destroy 幂等,重复调用安全。
- **destroy 后结构只读**:已销毁实例的变更方法(`splice`/`set`/`add`/`replace` 等)一律 no-op(dev 构建打印警告),数据保持销毁当刻的快照;destroy 也会取消在途 async patch 的后续应用。销毁的派生结构从此不再接收 source 的任何更新。销毁后读取惰性 meta(`length`/`keys`/`size` 等)返回快照值,且不会留下常驻的活 effect。
- **用户回调抛错不冻结状态机**:`onRecompute`/`onCleanup`/`context.onCleanup` 注册的清理在重算生命周期内抛错时,与 getter 抛错同一处置——错误同步抛给触发变更的调用方(async 路径 `console.error` 兜底),computed 复位为脏,后续任何触发都会全量重算追平终态,不会停留在"永久陈旧/每次写入抛误导性断言"的卡死状态。
- 在 `autorun`/`computed` getter 内创建的响应式对象会被收集为 child,随宿主重算/销毁自动清理(包括其 `context.onCleanup` 注册的清理与惰性 meta);不希望被收集时用 `ReactiveEffect.createDetached`。
- `map` 的 `context.onCleanup` 注册行级清理,随行移动,行删除/替换/整体销毁时各执行一次;`map` 回退全量重算时,旧行的 cleanup 同样各执行一次。行含响应式依赖而发生**行级重算**时,重算前先执行上一轮注册的清理(与 computed 的 `context.onCleanup`"每轮重算前执行"语义一致)——每轮注册恰好执行一次,行删除时执行的是最后一轮的注册。

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
| `RxSet.difference/intersection/symmetricDifference/union` | add/delete/replace 均增量(`replace` 的 `newItems` 按 Set 语义去重) | — | — |
| `RxSet.toList` | add/delete 增量(序 ≡ Set 插入序);`replace` → 重算(迭代序随新容器,含存活成员的相对顺序) | — | — |

**多变更重放脚注**:矩阵格子描述"一次 digest 恰一条变更"的行为。一次 digest 积累多条变更(batch 多操作、自定义延迟调度器)时,triggerInfo 的操作时位置与重放时的终态 source 可能不一致。`groupBy`、`slice`、`findIndex`(及其派生 find/some/every)、`reduce`/`reduceToAtom`(纯尾插序列,判定与 index 都按操作时长度)经 **digest 重放内核**(`src/digestReplay.ts`,从终态逆向还原每条变更操作时的源状态快照)在多变更下保持增量;快照不可重建时自动回退全量重算——触发条件:`set` 的旧值为 `undefined`(合法 undefined 元素与越界扩长在协议内不可区分)、非稠密下标的 `set`、未知方法。`map` 在行使用 index atom(`mapFn(item, index)`)或行含响应式依赖时多变更回退;`concat` 多变更回退(多源 offset 依赖各源操作时长度),越界 `set` 使源段长度跳变时单变更也回退(防跨段错位),同一源占多个操作数位置(如 `a.concat(a)`)时恒回退(一条变更对应多个段,按段位置的增量无法同时表达);`toSorted` 在插入元素与既有元素等值(tie)、变更涉及 `undefined` 元素值、增量删除的 tie 组内存在 `Object.is` 可区分成员(如 `0` 与 `-0`)、或**单次批量变更超过阈值**(插入数+删除数 > 64 且「> 派生长度/4 或 > 4096」——排序列表的批量逐项增量是 O(k×m),超过实测交叉点后全量重算更快且下游代价相同)时回退。回退是正确性措施,结果不变,只损失该次增量性。`groupBy` 的批量 splice 走单遍分桶增量(每组至多一次删除 + 一次插入 splice,幸存组引用稳定),不回退。**RxSet(含派生 RxSet)按内容语义承诺,内部迭代序不属承诺面**;需要稳定可观察顺序时经 `toList` 物化为有序 RxList(其行序恒 ≡ 源 Set 当前迭代序,见上表)。

矩阵行为由固定 seed 的差分 fuzz(`__tests__/broadOperatorsFuzz.spec.ts` 覆盖 map/filter/toSorted/slice/concat/toSet/groupBy/findIndex/length/RxSet 运算(含 toList)/RxMap 派生,`__tests__/duplicateValuesFuzz.spec.ts` 覆盖重复值域,`__tests__/batchReplayFuzz.spec.ts` 覆盖 batch 多操作重放与 toSorted 等值 tie,`__tests__/deepReview2026H2Findings.spec.ts` 覆盖 selection 家族的重复 item 域与 toSorted 的 undefined 元素值域)与各专项 spec 共同钉住。**新增派生结构或新增源操作时,必须同步补全本矩阵与对应差分测试;矩阵中声明"增量"的格子必须有差分验证。**

## 架构语义(不作为缺陷)

以下行为经深度 review 评估后确认为架构绑定的既定语义,**不修**,详见 [AGENTS.md「架构决策与已知语义边界」](./AGENTS.md):

- **A1** 菱形依赖 glitch(中间值可观察、重复重算,终值收敛);
- **A2** `batch()` 内读 computed 得旧值;
- **A3** 构造/`replace` 零拷贝采纳外部容器引用。

其可执行定义在 `__tests__/architectureSemantics.spec.ts`。

## 构建产物与 dev 告警

npm 包内含 **dev/prod 双构建**,经 `exports` 的 `development` 条件分发:

- **默认(生产/无条件解析)**:`dist/data0.js`(ESM)/ `dist/data0.umd.cjs`(CJS),`__DEV__:false`——零断言零告警,与历史单构建完全一致。
- **`development` 条件**:`dist/data0.dev.js` / `dist/data0.dev.umd.cjs`,`__DEV__:true`——纯度探测、销毁后变更警告、全局不变量断言全部生效。Vite(≥5.1)与 webpack 5 的开发模式自动命中该条件;Node 用 `node --conditions=development`。
- 类型:`import` 条件走 `dist/index.d.ts`,`require` 条件走 `dist/index.d.cts`(node16/nodenext 解析下 CJS 类型不再被判"伪装成 ESM")。
- **双格式提示**:同一进程内不要混用 `import` 与 `require` 加载 data0(双包危害)——两份模块实例各持独立的 notifier/作用域栈,跨实例的依赖追踪静默失效。按 `exports` 解析的现代打包器不会混用;手写互操作时统一走一种格式。

## 开发与验证

```bash
pnpm install --frozen-lockfile
pnpm test --run        # 全量测试(含差分 fuzz、交错枚举、不变量自检)
pnpm type-check
pnpm build             # 双构建:prod + dev(--mode dev)+ .d.cts
pnpm exec vitest run --coverage
pnpm bench             # 性能基准(热路径改动必须跑)
```

- CI(`.github/workflows/ci.yml`)在每个 push/PR 上执行以上基线。
- dev 构建(`__DEV__`)内置全局不变量断言(作用域栈/追踪栈平衡、行级记账对齐),违约立即抛错;生产构建零开销。
- 贡献与 review 纪律见 [AGENTS.md](./AGENTS.md) 与 [CONTRIBUTING.md](./CONTRIBUTING.md)。

## License

[MIT](./LICENSE)
