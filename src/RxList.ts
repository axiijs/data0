import {
    ApplyPatchType,
    CallbacksType,
    computed,
    Computed,
    destroyComputed,
    DirtyCallback,
    GetterType,
    setComputedRetainedDiagnosticSource,
    STATUS_CLEAN
} from "./computed.js";
import {Atom, atom, isAtom} from "./atom.js";
import {Dep, isDepEmpty} from "./dep.js";
import {InputTriggerInfo, ITERATE_KEY, notifier, TriggerInfo} from "./notify.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {assert, normalizeSpliceDeleteCount, normalizeSpliceStart, spliceMany, toIntegerOrInfinity, warn} from "./util.js";
import {reconstructDigestStates} from "./digestReplay.js";
import {ReactiveEffect} from "./reactiveEffect.js";
import {RxMap} from "./RxMap.js";
import {RxSet} from "./RxSet";

type MapOptions<U> = {
    beforePatch?: (triggerInfo: InputTriggerInfo) => any,
    afterPatch?: (triggerInfos: TriggerInfo[]) => any,
    scheduleRecompute?: DirtyCallback,
    ignoreIndex?: boolean,
    onCleanup?: (item: U) => any,
    skipItemEffect?: boolean
}

type MapCleanupFn = () => any

// cleanup 槽位：随行移动的身份稳定容器（见 map() 中 cleanupSlots 的说明）
type MapCleanupSlot = { fn?: MapCleanupFn }

type MapContext = {
    onCleanup: (fn: MapCleanupFn) => void
}

/**
 * RxList.map 的行级依赖探测 effect（每个 map 产物复用一个实例）。
 *
 * 旧实现为每一行 new 一个 Computed、run 一遍、再对（最常见的）无依赖行 destroy，
 * 一行的固定成本是完整的 Computed 构造 + 重算状态机 + 销毁。探测方案下 mapFn 仍然
 * 恰好执行一次（在 probe 的 tracking 作用域里），只有真正捕获到依赖/子 effect 的行
 * 才升级为常驻的行级 Computed（订阅关系原样转移，见 transferCapturesTo），
 * 无依赖行的成本只剩 prepareTracking/completeTracking 一对。
 */
class MapItemDependencyProbe extends ReactiveEffect {
    fn?: () => any
    constructor() {
        super()
        this.active = true
    }
    callGetter() {
        return this.fn!()
    }
    probe(fn: () => any) {
        // 上一次 mapFn 抛异常时 deps 可能残留，复用前防御性清理
        if (this.deps.length) this.cleanup()
        this.fn = fn
        try {
            return this.run()
        } finally {
            this.fn = undefined
        }
    }
    hasCaptures() {
        return this.deps.length > 0 || this.hasChildren()
    }
}

// 无依赖行共享同一个 frozen 空 frame，长列表少一次每行的空数组分配；
// freeze 保证未来误往共享 frame push 会立刻抛错而不是跨行污染。
const EMPTY_ITEM_FRAME = Object.freeze([]) as unknown as ReactiveEffect[]

/**
 * dev 不变量：atomIndexes 不长于 data，且每个**存在**的 entry 第 i 个 atom 的值恒为 i。
 * 违约意味着行级 index 记账出错（map 行会拿到错误位置），在变更当刻炸掉,
 * 而不是等下游行为漂移。模块级函数且仅在 __DEV__ 分支引用：
 * 生产构建连函数体一起被 DCE 移除，零运行时与零体积开销。
 * 全量值扫描是 O(n)，仅存在 atomIndexes（有行级 index 订阅）时发生。
 * CAUTION 容忍稀疏：越界 set 会让 data 出洞；set 只为**写入的** index 分配
 *  atomIndex（洞位可空）。"更长"或"存在的值漂移"才是 splice/reorder 记账缺陷。
 */
function assertAtomIndexesAligned(list: RxList<any>) {
    if (!list.atomIndexes) return
    assert(list.atomIndexes.length <= list.data.length, 'atomIndexes longer than data')
    for (let i = 0; i < list.atomIndexes.length; i++) {
        const indexAtom = list.atomIndexes[i]
        if (indexAtom) assert(indexAtom.raw === i, `atomIndex value drift at ${i}`)
    }
}

/**
 * EKC(set)的 key 是否是稠密下标。负/小数/非数字 key 的 set 契约上等同数组
 * 属性赋值:不触及任何行元素,全量 computation(只遍历 [0, length))也看不见它。
 * 派生结构的 patch 端凡按 key 物化行/成员的,都必须先过本谓词——否则幽灵 EKC
 * 会把"属性赋值"物化成真实行(filter 插幽灵行、groupBy/indexBy/toMap/toSet 加
 * 幽灵成员;2026-H3 round3 形态操作 fuzz 命中)。协议侧 key 仍原样透传
 * (README「RxList 参数契约」),内部消费者独立归一化——与 splice argv 同规则。
 */
function isDenseIndexKey(key: unknown): key is number {
    return typeof key === 'number' && Number.isInteger(key) && key >= 0
}

export type Order = [number, number]
export type ReorderKind = 'swap' | 'move' | 'sort' | 'reorder'
export type ReorderPatchInfo = {
    kind: ReorderKind,
    affectedRange: [number, number] | null,
    movedCount: number,
    oldIndexToNewIndex: Map<number, number>,
    start?: number,
    newStart?: number,
    limit?: number,
}

function createReorderPatchInfo(kind: ReorderKind, newOrder: Order[], details: Partial<ReorderPatchInfo> = {}): ReorderPatchInfo {
    let minMovedIndex = Infinity
    let maxMovedIndex = -Infinity
    let movedCount = 0
    const oldIndexToNewIndex = new Map<number, number>()

    for (let i = 0; i < newOrder.length; i++) {
        const [oldIndex, newIndex] = newOrder[i]!
        oldIndexToNewIndex.set(oldIndex, newIndex)
        if (oldIndex === newIndex) continue

        movedCount++
        if (oldIndex < minMovedIndex) minMovedIndex = oldIndex
        if (newIndex < minMovedIndex) minMovedIndex = newIndex
        if (oldIndex > maxMovedIndex) maxMovedIndex = oldIndex
        if (newIndex > maxMovedIndex) maxMovedIndex = newIndex
    }

    return {
        kind,
        affectedRange: movedCount
            ? [minMovedIndex, maxMovedIndex]
            : null,
        movedCount,
        oldIndexToNewIndex,
        ...details,
    }
}

/**
 * @category Basic
 *
 * @noInheritDoc
 */
export class RxList<T> extends Computed {
    get raw() { return this.data }
    data!: T[]
    /**
     * @internal
     */
    trackClassInstance = true
    /**
     * @internal
     */
    _indexKeyDeps?: Map<number, Dep>
    get indexKeyDeps(): Map<number, Dep> {
        return this._indexKeyDeps ?? (this._indexKeyDeps = new Map())
    }
    /**
     * @internal
     * 清扫已无订阅者的 index dep，返回是否仍有活跃订阅。
     * CAUTION 悬崖修复：at() 建立的 index dep 在 effect 全部退订后 Map entry 仍在，
     *  旧实现据此永久走 splice 的逐 index 触发慢路径（也是缓慢的内存增长）。
     *  订阅者退订不会回调 RxList（Notifier 不知道 dep 的归属），所以在每次
     *  结构变更入口做一次 O(订阅数) 的惰性清扫——这些 entry 本来也要被遍历。
     */
    pruneIndexKeyDeps(): boolean {
        const indexDeps = this._indexKeyDeps
        if (!indexDeps || indexDeps.size === 0) return false
        // CAUTION 与 notifier 的记账残留摘除（pruneEmptyDepFromHost）联动：
        //  index dep 退订清空后可能已被从 depsMap 摘除，甚至同 index 被重新订阅
        //  重建成了新 dep。缓存必须以 depsMap 的现行 dep 为准重同步——只按持有的
        //  旧 dep 判空会误删（旧 dep 空、新 dep 有订阅者），splice 的受影响区间
        //  从此漏触发该 index。重同步放在结构变更入口（这些 entry 本来就要遍历），
        //  at() 的读热路径保持零额外开销。
        const depsMap = notifier.targetMap.get(this)
        for (const [index, dep] of indexDeps) {
            const current = depsMap?.get(index)
            if (current === undefined) {
                indexDeps.delete(index)
            } else if (current !== dep) {
                indexDeps.set(index, current)
            } else if (isDepEmpty(dep)) {
                indexDeps.delete(index)
            }
        }
        return indexDeps.size > 0
    }
    /**
     * @internal
     */
    atomIndexes? :Atom<number>[]
    /**
     * @internal
     */
    atomIndexesDepCount = 0

    constructor(sourceOrGetter?: T[]|null|GetterType, public applyPatch?: ApplyPatchType, scheduleRecompute?: DirtyCallback, public callbacks? : CallbacksType) {
        const getter = typeof sourceOrGetter === 'function' ? sourceOrGetter : undefined
        const source = typeof sourceOrGetter !== 'function' ? sourceOrGetter : undefined

        // 自己可能是 computed，也可能是最初的 reactive
        super(getter, applyPatch, scheduleRecompute, callbacks)
        this.getter = getter

        // 自己是 source
        // CAUTION 架构语义（AGENTS.md「架构决策与已知语义边界」A3）：直接采纳传入
        //  数组（零拷贝，所有权移交）。调用方之后必须通过本实例的方法修改；绕过
        //  方法直改原数组不会触发任何通知。刻意不做防御性拷贝，明确不修。
        this.data = source || []
        if (this.getter) {
            this.run([], true)
        }
    }
    /**
     * @internal
     */
    replaceData(newData: any[]) {
        // 这里的 newData type 为 any[]，是为了让子类能覆写，实现 replaceData 的时候才进行数据转换。
        // CAUTION 数组参数版：spread 传参对大列表（>65k）会超出实参上限直接 RangeError。
        this.spliceArray(0, this.data.length, newData)
    }

    push(...items: T[]) {
        return this.splice(this.data.length, 0, ...items)
    }
    clear() {
        if (!this.active) {
            warn('mutating a destroyed RxList is a no-op')
            return []
        }
        const length = this.data.length
        if (length === 0) return []
        const hasIndexKeyDeps = this.pruneIndexKeyDeps()
        const hasAtomIndexes = !!this.atomIndexes
        if (length === 1 || hasIndexKeyDeps || hasAtomIndexes) return this.splice(0, length)

        this.pauseAutoTrack()
        // CAUTION try/finally：sendTriggerInfos 会同步执行订阅者，订阅者抛错时
        //  必须复位追踪状态，否则 trackStack 每次异常泄漏一个槽位、顶层 shouldTrack
        //  永久卡在 false。
        try {
            const deletedItems = this.data.slice()
            this.data.length = 0
            this.trigger(this, TriggerOpTypes.METHOD, { method:'splice', key: ITERATE_KEY, argv: [0, length], methodResult: deletedItems })
            this.sendTriggerInfos()
            return deletedItems
        } finally {
            this.resetAutoTrack()
        }
    }
    pop( ) {
        return this.splice(this.data.length - 1, 1)[0]
    }
    shift( ) {
        return this.splice(0, 1)[0]
    }
    unshift( ...items: T[]) {
        return this.splice(0, 0, ...items)
    }
    splice( start: number, deleteCount: number, ...items: T[]) {
        return this.spliceArray(start, deleteCount, items)
    }
    // splice 的数组参数版：内部所有批量写入都走这里，规避 spread 实参上限
    // （大列表 replaceData/concat 等场景 spread 会 RangeError）与 O(n) 实参拷贝。
    spliceArray(start: number, deleteCount: number, items: T[] = []) {
        // CAUTION 已销毁实例的变更是 no-op：destroy 无法中止已挂起的用户 async
        //  applyPatch，其恢复后对 this 的写入必须在这里被拦截（复活写入防线）。
        //  同时兜住所有"销毁后仍持有引用继续写"的契约外用法。
        if (!this.active) {
            warn('mutating a destroyed RxList is a no-op')
            return []
        }
        this.pauseAutoTrack()
        // CAUTION try/finally：trigger/digest 会同步执行订阅者，订阅者抛错时必须
        //  复位追踪状态，否则 trackStack 每次异常泄漏一个槽位、顶层 shouldTrack
        //  永久卡在 false。
        try {
            return this.doSplice(start, deleteCount, items)
        } finally {
            this.resetAutoTrack()
        }
    }
    /**
     * @internal
     * CAUTION 构造性不变量（2026-H3 round4，R4-1 等价类）：结构变更的 info 派发
     *  必须先于"订阅者可见的原子写"（index atom 值写入等 meta 维护）对外可见。
     *  否则非 batch 下原子写会同步执行行级订阅者（map 的 rowComputed），此刻它们
     *  看不到任何 pending 结构 info（hasPendingStructuralInfos 守卫失效），按终态
     *  位置直写派生列表，随后派生列表的结构 patch 再搬移一次——双重搬移，silent
     *  乱序。本原语把顺序固化为：同一 effect session 内先 sendTriggerInfos、再执行
     *  原子写；digest 时结构 patch 先应用、行级重算后运行。
     *  所有"派发结构 info + 随后写 meta 原子"的变更方法必须走本原语，禁止各自
     *  手写 createEffectSession/sendTriggerInfos 顺序——doSplice 曾正确、reorder
     *  曾遗漏，同一不变量的兄弟实现点不一致即缺陷（AGENTS「不变量升格」规则的
     *  第一个登记项，静态执法见 __tests__/sourceInvariants.spec.ts）。
     */
    protected dispatchStructuralThen(atomWrites?: () => void) {
        notifier.createEffectSession()
        try {
            this.sendTriggerInfos()
            atomWrites?.()
        } finally {
            // CAUTION 放在 finally：session 一旦创建必须消化，否则 notifier 永久
            //  停留在 session 模式，之后所有 trigger 都被吞进队列。
            notifier.digestEffectSession()
        }
    }
    private doSplice(start: number, deleteCount: number, items: T[]) {
        const originLength = this.data.length
        // CAUTION 内部计算一律用归一化后的 start/deleteCount（Array#splice 的
        //  ToIntegerOrInfinity + clamp 语义）：负/越界/小数 start 直接参与区间计算会算错
        //  受影响的 index（订阅了 at(index) 的 effect 收不到 SET 而静默保持旧值），
        //  也会给 atomIndexes 写入负下标。
        //  triggerInfo.argv 仍按契约透传用户原始参数（axii/axle 锁定该行为并自行归一化），
        //  data0 自己的派生结构（map/findIndex/concat 等）在 patch 端归一化后再消费。
        const normalizedStart = normalizeSpliceStart(start, originLength)
        const deleteItemsCount = normalizeSpliceDeleteCount(deleteCount, originLength, normalizedStart)
        // 清扫空 index dep：曾被 at() 订阅、现已全部退订的列表要能回到 fast path
        const hasIndexKeyDeps = this.pruneIndexKeyDeps()
        const hasAtomIndexes = !!this.atomIndexes
        const canUseMetadataFastPath = !hasIndexKeyDeps && !hasAtomIndexes
        const isPureAppend = normalizedStart === originLength && deleteItemsCount === 0
        const isPureClear = normalizedStart === 0 && deleteItemsCount >= originLength && items.length === 0

        if (canUseMetadataFastPath && (isPureAppend || isPureClear)) {
            const result = spliceMany(this.data, normalizedStart, deleteItemsCount, items)
            this.trigger(this, TriggerOpTypes.METHOD, { method:'splice', key: ITERATE_KEY, argv: [start, deleteCount, ...items], methodResult: result })
            this.sendTriggerInfos()
            return result
        }


        // CAUTION 不需要触发 length 的变化，因为获取  length 的时候得到就已经是个 computed 了。
        const newLength = originLength - deleteItemsCount + items.length
        // CAUTION 受影响区间上界必须覆盖收缩场景：newLength < originLength 时
        //  [newLength, originLength) 的 index 从有值变成 undefined，订阅了这些 index
        //  的 at() effect（例如订阅末位后 pop）也必须收到 SET，否则永久读到旧值。
        const changedIndexEnd = deleteItemsCount !== items.length
            ? Math.max(newLength, originLength)
            : normalizedStart + items.length
        // CAUTION 只对"实际有订阅者且落在受影响区间"的 index 记录 oldValue / 触发 SET：
        //  旧实现对 [start, changedIndexEnd) 逐 index 触发，一次中段 splice 是 O(移动范围)
        //  次 trigger；订阅通常是稀疏的（axii 每行订阅自己的 index），按订阅遍历后
        //  复杂度变为 O(订阅数)。触发保持升序，与旧实现的可观察顺序一致。
        let affected: [index: number, oldValue: T][] | undefined
        if (hasIndexKeyDeps) {
            for (const index of this._indexKeyDeps!.keys()) {
                if (index >= normalizedStart && index < changedIndexEnd) {
                    (affected ?? (affected = [])).push([index, this.data[index]])
                }
            }
            if (affected && affected.length > 1) {
                affected.sort((a, b) => a[0] - b[0])
            }
        }
        const result = spliceMany(this.data, normalizedStart, deleteItemsCount, items)


        // CAUTION 无论有没有 indexKeyDeps 都要触发 Iterator_Key，
        //  特别这里注意，我们利用传了 key 就会把对应 key 的 dep 拿出来的特性来 trigger ITERATE_KEY.
        //  CAUTION 一定先 trigger method，这样可能后面某些被删除的 atomIndexes 变化就不需要了。
        this.trigger(this, TriggerOpTypes.METHOD, { method:'splice', key: ITERATE_KEY, argv: [start, deleteCount, ...items], methodResult: result })
        if (affected) {
            for (const [index, oldValue] of affected) {
                this.trigger(this, TriggerOpTypes.SET, { key: index, newValue: this.data[index], oldValue })
            }
        }

        // 结构 info 先派发、atomIndexes 维护随后（顺序契约见 dispatchStructuralThen）
        this.dispatchStructuralThen(this.atomIndexes === undefined ? undefined : () => {
            // CAUTION 稀疏对齐：越界 set（契约内透传）会让 data 变长而 atomIndexes
            //  没跟上。此时必须先把 atomIndexes 撑到 splice 前的 data 长度（产生洞,
            //  零分配），否则 native splice 会把 start 钳到旧长度，新 index atom
            //  全部插错位置（值与位置漂移）。洞位置不分配 atom（与初始构建时
            //  Array#map 跳过稀疏洞的行为一致），由下方 ?. 跳过。
            if (this.atomIndexes!.length < originLength) this.atomIndexes!.length = originLength
            spliceMany(this.atomIndexes!, normalizedStart, deleteItemsCount, items.map((_, index) => atom(index + normalizedStart)))
            for (let i = normalizedStart; i <changedIndexEnd; i++) {
                // 注意这里的 ?. ，因为 splice 之后可能长度不够了。
                this.atomIndexes![i]?.(i)
            }
        })

        if (__DEV__) assertAtomIndexesAligned(this)
        return result
    }
    // 显式 set 某一个 index 的值
    // CAUTION set 的契约是"替换已存在的稠密行"。越界/负数/非整数 key 属于 out-of-contract 用法：
    //  行为与普通数组赋值一致（可能产生稀疏数组、length computed 不会更新），trigger 原样透传
    //  key，由下游（axii/axle 等渲染框架）自行拒绝或归一化。不要在这里改走 splice 语义：
    //  下游的结构化错误契约（以及 set(Infinity) 这类 key）都依赖透传行为。
    // CAUTION 若已有 atomIndexes（map 使用了 index），越界 set 必须在 trigger 前为写入位
    //  分配 index atom，并允许 atomIndexes 出洞与 data 对齐——否则 map(index) patch/
    //  全量重算会对 undefined 调用 mapFn，抛 TypeError 并永久毒化派生链。
    set(index: number, value: T) {
        if (!this.active) {
            warn('mutating a destroyed RxList is a no-op')
            return undefined as T
        }
        const oldValue = this.data[index]
        this.data[index] = value

        if (this.atomIndexes && Number.isInteger(index) && index >= 0) {
            this.ensureAtomIndex(index)
        }

        // 这里还是用 trigger TriggerOpTypes.SET，因为系统在处理 TriggerOpTypes.SET 的时候还会对 listLike 的数据 触发 ITERATE_KEY。
        this.trigger(this, TriggerOpTypes.SET, { key: index, newValue: value, oldValue})
        this.trigger(this, TriggerOpTypes.EXPLICIT_KEY_CHANGE, { key: index, newValue: value, oldValue, methodResult: oldValue})
        this.sendTriggerInfos()

        if (__DEV__) assertAtomIndexesAligned(this)
        return oldValue
    }
    /**
     * @internal
     * 保证 atomIndexes[index] 存在（必要时撑长并分配）。洞位不预先分配。
     */
    ensureAtomIndex(index: number): Atom<number> {
        if (!this.atomIndexes) this.atomIndexes = []
        if (this.atomIndexes.length <= index) this.atomIndexes.length = index + 1
        return this.atomIndexes[index] ?? (this.atomIndexes[index] = atom(index))
    }
    reorder(newOrder: Order[], reorderInfo = createReorderPatchInfo('reorder', newOrder)) {
        if (!this.active) {
            warn('mutating a destroyed RxList is a no-op')
            return
        }
        const originIndexes = newOrder.map(item => item[0])
        const newIndexes = newOrder.map(item => item[1])
        const oldIndexAtoms = this.atomIndexes ? originIndexes.map(index => this.atomIndexes![index]) : null
        // 要不要触发 set 语义呢？理论上是需要的
        const originItems = originIndexes.map(index => this.data[index])
        const originItemsInNewIndexes = newIndexes.map(index => this.data[index])
        // 只对实际有订阅者的 index 触发 SET（清扫见 pruneIndexKeyDeps）
        const hasIndexKeyDeps = this.pruneIndexKeyDeps()
        newIndexes.forEach((newIndex, i) => {
            this.data[newIndex]= originItems[i]
            if (hasIndexKeyDeps && this._indexKeyDeps!.has(newIndex)) {
                this.trigger(this, TriggerOpTypes.SET, { key: newIndex, newValue: originItems[i], oldValue: originItemsInNewIndexes[i]})
            }
            if (oldIndexAtoms) {
                this.atomIndexes![newIndex] = oldIndexAtoms[i]!
            }
        })

        this.trigger(this, TriggerOpTypes.METHOD, { method:'reorder', key: ITERATE_KEY, argv: [newOrder], reorderInfo })
        // CAUTION 结构 info 必须先于 index atom 的值写入对订阅者可见：否则非 batch 下
        //  原子写同步执行 map 的行级 Computed，其 hasPendingStructuralInfos 守卫看不到
        //  pending 结构 info，按终态位置直写派生列表，随后 reorder patch 又搬移一次
        //  ——双重搬移，silent 乱序（2026-H3 round4 动态复现）。顺序由
        //  dispatchStructuralThen 原语固化（与 doSplice 同一出口）。
        this.dispatchStructuralThen(oldIndexAtoms === null ? undefined : () => {
            newIndexes.forEach((newIndex, i) => {
                oldIndexAtoms[i]?.(newIndex)
            })
        })
        if (__DEV__) assertAtomIndexesAligned(this)
    }
    reposition(start:number, newStart:number, limit:number = 1 ) {
        assert(start >= 0 && limit > 0 && start+limit <= this.data.length, 'start index out of range')
        assert(newStart >= 0 && newStart+limit <= this.data.length, 'newStart index out of range')
        // 1. 如果是往前移动，新位置到原来为止中间的元素都要往后移动
        // 2. 如果是往后移动，原来位置到新位置为止中间的元素都要往前移动
        if (start === newStart) return
        const newOrder:Order[] = []
        for (let i = 0; i < limit; i++) {
            newOrder.push([start + i, newStart + i])
        }

        // 往前
        if (newStart < start) {
            for(let i = newStart; i < start; i++) {
                newOrder.push([i, i + limit])
            }
        } else {
            // 往后
            for(let i = start + limit; i < newStart+limit; i++) {
                newOrder.push([i, i - limit])
            }
        }
        return this.reorder(newOrder, createReorderPatchInfo('move', newOrder, { start, newStart, limit }))
    }
    swap(start: number, newStart:number, limit:number = 1) {
        assert(start >= 0 && limit > 0 && start+limit <= this.data.length, 'start index out of range')
        assert(newStart >= 0 && newStart+limit <= this.data.length, 'newStart index out of range')
        const newOrder:Order[] = []
        for (let i = 0; i < limit; i++) {
            newOrder.push([start + i, newStart + i])
            newOrder.push([newStart + i, start + i])
        }
        return this.reorder(newOrder, createReorderPatchInfo('swap', newOrder, { start, newStart, limit }))
    }
    sortSelf(compare: (a: T, b:T)=> number) {
        const sortedOldIndexes = new Array<number>(this.data.length)
        for (let index = 0; index < this.data.length; index++) {
            sortedOldIndexes[index] = index
        }
        sortedOldIndexes.sort((a, b) => compare(this.data[a]!, this.data[b]!))

        const newOrder = new Array<Order>(sortedOldIndexes.length)
        for (let newIndex = 0; newIndex < sortedOldIndexes.length; newIndex++) {
            newOrder[newIndex] = [sortedOldIndexes[newIndex]!, newIndex]
        }
        return this.reorder(newOrder, createReorderPatchInfo('sort', newOrder))
    }
    private static binarySearchInsert<S>(arr: S[], item: S, compare: (a: S, b: S) => number): number {
        // A simple binary search to find the insertion index
        let low = 0
        let high = arr.length
        while (low < high) {
            const mid = (low + high) >>> 1
            if (compare(arr[mid], item) <= 0) {
                low = mid + 1
            } else {
                high = mid
            }
        }
        return low
    }
    // 在按 compare 有序的数组中定位与 item 同一身份的元素：
    // 二分到相等区间后线性扫描（同序元素可能有多个）。
    // CAUTION 身份比较必须用 Object.is 而不是 ===/indexOf（2026-H2 NaN/-0 值域
    //  sweep 动态复现的缺陷类，与 RxSet.toList/RxMap.keys 的 F10 同源）：
    //  - NaN === NaN 为 false，=== 与 indexOf 都永远找不到 NaN → 删除被静默跳过，
    //    派生列表 NaN 残留；
    //  - 0 === -0 为 true，compare 等值区间内会命中错误实例（把 -0 当 0 删掉），
    //    实例组成与全量重算漂移。Object.is 对两者都给出精确身份。
    private static binarySearchFind<S>(arr: S[], item: S, compare: (a: S, b: S) => number): number {
        let low = 0
        let high = arr.length
        while (low < high) {
            const mid = (low + high) >>> 1
            if (compare(arr[mid], item) < 0) {
                low = mid + 1
            } else {
                high = mid
            }
        }
        for (let i = low; i < arr.length && compare(arr[i], item) === 0; i++) {
            if (Object.is(arr[i], item)) return i
        }
        return -1
    }
    // 找不到（元素排序键在删除前被外部改写、数组不完全有序）时全数组 Object.is
    // 扫描兜底；仍找不到返回 -1，调用方必须回退全量重算（增量状态已不可信）。
    private static locateInSorted<S>(arr: S[], item: S, compare: (a: S, b: S) => number): number {
        const found = RxList.binarySearchFind(arr, item, compare)
        if (found !== -1) return found
        for (let i = 0; i < arr.length; i++) {
            if (Object.is(arr[i], item)) return i
        }
        return -1
    }

    public toSorted(compare?: (a: T, b: T) => number): RxList<T> {
        const source = this
        // default compare if not provided
        compare = compare ?? ((a, b) => {
            if (a < b) return -1
            if (a > b) return 1
            return 0
        })

        return new RxList<T>(
            function computation(this: RxList<T>) {
                // Full recompute: track source changes
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
                // Make a clone of source.data and sort it
                const cloned = source.data.slice()
                cloned.sort(compare!)
                return cloned
            },
            function applyPatch(this: RxList<T>, _data, triggerInfos) {
                // CAUTION tie 回退：全量重算是稳定排序（等 key 按源顺序），增量二分插入
                //  无法得知新元素相对既有等 key 元素的源顺序。等 key 元素存在时直接
                //  return false 走全量重算，保证"增量 ≡ 全量重算"的差分不变量
                //  （否则同一份数据的呈现顺序取决于到达历史）。binarySearchInsert
                //  返回等值区间之后的位置，因此只需检查前一个元素是否等值。
                const insertOrBail = (item: T): boolean => {
                    const insertIndex = RxList.binarySearchInsert(this.data, item, compare!)
                    if (insertIndex > 0 && compare!(this.data[insertIndex - 1], item) === 0) return false
                    this.splice(insertIndex, 0, item)
                    return true
                }
                // CAUTION 删除侧的 tie 歧义(2026-H2 NaN/-0 值域 sweep 动态复现,与插入
                //  侧 tie 回退同一等价类):被删值在 tie 组内有 ≥2 个 Object.is 相同的
                //  副本,且组内还存在 compare-相等但 Object.is 可区分的其他成员(0 与
                //  -0、compare 按字段相等的不同字符串/对象)时,"删哪个副本"会改变组内
                //  剩余序——增量无从得知源顺序,而全量稳定排序由源顺序决定。此时回退
                //  全量重算。纯重复值组(无可区分成员)与身份精确的对象删除不受影响,
                //  保持增量。定位失败(排序键被外部改写)同样回退。
                const removeOrBail = (item: T): boolean => {
                    const idx = RxList.locateInSorted(this.data, item, compare!)
                    if (idx === -1) return false
                    let hasDuplicateOfItem = false
                    let hasDistinctTieMember = false
                    for (let j = idx - 1; j >= 0 && compare!(this.data[j], item) === 0; j--) {
                        if (Object.is(this.data[j], item)) hasDuplicateOfItem = true
                        else hasDistinctTieMember = true
                    }
                    for (let j = idx + 1; j < this.data.length && compare!(this.data[j], item) === 0; j++) {
                        if (Object.is(this.data[j], item)) hasDuplicateOfItem = true
                        else hasDistinctTieMember = true
                    }
                    if (hasDuplicateOfItem && hasDistinctTieMember) return false
                    this.splice(idx, 1)
                    return true
                }
                // Incremental updates
                // CAUTION undefined 是 RxList 的合法元素值，但增量路径无法正确处理它：
                //  1) Array#sort 的全量语义从不对 undefined 调用 compare（undefined 一律
                //     排到尾部），增量二分却会把 undefined 喂给 compare——数值型 compare
                //     返回 NaN 时二分插到错误位置，与全量重算分叉；
                //  2) explicit key change 的 oldValue/newValue 用 `!== undefined` 当
                //     "有无"判断，无法区分"值为 undefined"与"无值"——set 引入 undefined
                //     会丢行、替换 undefined 会残留行。
                //  变更涉及 undefined 时一律回退全量重算（结果与 Array#sort 语义一致，
                //  只损失该次增量性；同 tie 回退的处置）。
                for (const info of triggerInfos) {
                    const { method, argv, key, type, oldValue, newValue, methodResult } = info
                    // method could be 'splice' (array insertion/removal) or an explicit key change
                    if (method === 'splice') {
                        const deletedItems = (methodResult as T[]) || []
                        const newItems = argv!.slice(2) as T[]
                        // includes 是 SameValueZero：undefined（含稀疏洞读出的 undefined）可命中
                        if (deletedItems.includes(undefined as T) || newItems.includes(undefined as T)) return false
                        // 1) remove items（tie 歧义与定位失败都回退全量,见 removeOrBail）
                        for (const item of deletedItems) {
                            if (!removeOrBail(item)) return false
                        }
                        // 2) insert new items in sorted order
                        for (const item of newItems) {
                            if (!insertOrBail(item)) return false
                        }
                    } else if (type !== TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // reorder（method='reorder'）等非 EKC：成员不变但源序变——tie 组的
                        // 稳定序依赖源序，必须回退全量重算。旧实现靠 else 分支的
                        // oldValue===undefined 检查偶然兜住，这里显式化（不得被下方的
                        // 非稠密 key 守卫吞掉——reorder 的 key 是 ITERATE_KEY Symbol）。
                        return false
                    } else {
                        // 非稠密 key 的 set 是数组属性赋值,不触及任何元素:忽略 ≡ 全量重算
                        if (!isDenseIndexKey(key)) continue
                        // explicit key change: remove old, insert new
                        // 值为 undefined（含越界 set 的"无旧值"）→ 回退全量重算
                        if (oldValue === undefined || newValue === undefined) return false
                        if (!removeOrBail(oldValue as T)) return false
                        if (!insertOrBail(newValue as T)) return false
                    }
                }
            }
        )
    }
    // CAUTION 这里手动 track index dep 的变化，是为了在 splice 的时候能手动去根据订阅的 index dep 触发，而不是直接触发所有的 index key。
    at(index: number): T|undefined{
        // 与 Array.prototype.at 一致：支持负索引（从末尾回数）。
        // CAUTION 负索引的结果依赖 length，不能建 index dep（元素本身没变时对应
        //  index 不会触发 SET），track ITERATE_KEY——所有结构变更（splice/reorder/set）
        //  都会通知它。
        if (index < 0) {
            notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
            return this.data[this.data.length + index]
        }
        const dep = notifier.track(this, TrackOpTypes.GET, index)
        // 缓存可能因记账摘除而过期（dep 被 pruneEmptyDepFromHost 移除后同 index
        // 重新订阅会建新 dep），由 pruneIndexKeyDeps 在结构变更入口统一重同步，
        // 这里保持读热路径零额外开销。
        if (dep && !this.indexKeyDeps.has(index)) {
            this.indexKeyDeps.set(index, dep)
        }
        // CAUTION 这里不做深度的 reactive 包装
        return this.data[index]
    }

    // CAUTION 遍历型读取只 track ITERATE_KEY，不再为每个 index 建 dep：
    //  所有变更路径都必然通知 ITERATE_KEY 订阅者（splice/reorder → METHOD(key=ITERATE_KEY)，
    //  set → SET 会附带 ITERATE_KEY dep，见 notify.trigger 的 SET case），逐 index track
    //  是纯冗余——一次 forEach 会建立 O(n) 个 index dep（重算时 O(n) 的 marker 双遍历），
    //  并把列表永久推入 splice 的逐 index 触发慢路径。
    //  index 级细粒度依赖仍由显式 at() 提供。
    forEach(handler: (item: T, index: number) => void) {
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
        const data = this.data
        for (let i = 0; i < data.length; i++) {
            handler(data[i], i)
        }
    }
    /**
     * @internal
     */
    [Symbol.iterator](): IterableIterator<T> {
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
        // 直接用原生数组迭代器（引擎优化，不逐步分配 result 对象）。
        // 旧的手写 iterator 从不递增 index，for...of 非空列表会死循环。
        return this.data[Symbol.iterator]()
    }
    /**
     * @internal
     */
    addAtomIndexesDep() {
        if (!this.atomIndexes) this.atomIndexes = this.data.map((_, index) => atom(index))
        this.atomIndexesDepCount++
    }
    /**
     * @internal
     */
    removeAtomIndexesDep() {
        this.atomIndexesDepCount--
        if (this.atomIndexesDepCount === 0) {
            this.atomIndexes = undefined
        }
    }

    // reactive methods and attr
    map<U>(mapFn: (item: T, index: Atom<number>, context:MapContext) => U, options?: MapOptions<U>) : RxList<U>{
        // CAUTION 生成数据结构的方法应该都不 track Iterable_Key。不然可能导致在 computed 里面的 map 方法被反复执行，这算是一种泄露了。
        // notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)

        const source = this
        const useIndex = mapFn.length>1 && !options?.ignoreIndex
        const useContext = mapFn.length>2
        assert(!options?.skipItemEffect || !useIndex, 'skipItemEffect can not be used with index')
        if(useIndex) {
            source.addAtomIndexesDep()
        }

        let addedAtomIndexesDep = useIndex

        // CAUTION cleanup 以"槽位对象"随行移动，而不是按 index 记账：
        //  1. onCleanup 闭包捕获槽位对象本身（身份稳定），行移动后重算再注册仍写进
        //     自己的槽位；旧实现闭包捕获创建时的 index，行移动后写回旧位置，
        //     删除行时会执行到别人的 cleanup（错误释放）或过期的 cleanup（漏释放）。
        //  2. 槽位数组与行严格等长（未注册的行占一个空槽），patch 的 splice 对齐
        //     不再受"部分行未注册"的稀疏数组影响。
        //  如果可以直接从 mapFn return value 中来销毁副作用，那么应该使用
        //  options.onCleanup 注册统一销毁函数，避免逐行槽位分配。
        let cleanupSlots: MapCleanupSlot[]|undefined
        function createRowContext(): {slot: MapCleanupSlot, context: MapContext} {
            const slot: MapCleanupSlot = {}
            return {
                slot,
                context: {
                    onCleanup(fn: MapCleanupFn) {
                        slot.fn = fn
                    }
                }
            }
        }
        // 行级依赖探测 effect：整个 map 产物复用一个实例（说明见类定义处）。
        // detached 创建：不能被 map computation 收集为 child。
        let itemProbe: MapItemDependencyProbe | undefined

        // 探测一行：mapFn 恰好执行一次；捕获到依赖/子 effect 时升级为常驻的行级
        // Computed（探测期间建立的订阅原样转移），无依赖行零对象分配。
        // 返回 [mapFn 返回值, 行级 effect frame]。
        function indexAtomFor(index: number): Atom<number> {
            // useIndex 或行级依赖升级后都会需要可靠的 index atom；越界 set 造成的
            // 稀疏洞不能把 undefined 传给用户 mapFn。
            return source.ensureAtomIndex(index)
        }

        function runItemAndCollectEffect(list: RxList<U>, item: T, index: number, mapContext: MapContext | undefined): [U, ReactiveEffect[]] {
            const probe = itemProbe ?? (itemProbe = ReactiveEffect.createDetached(() => new MapItemDependencyProbe()))
            const value = probe.probe(() => mapFn(item, useIndex || addedAtomIndexesDep ? indexAtomFor(index) : source.atomIndexes?.[index]!, mapContext!)) as U
            if (!probe.hasCaptures()) return [value, EMPTY_ITEM_FRAME]

            // CAUTION 只有依赖变化需要重算的行才需要 index atom（重算时行的位置可能已变化）；
            //  仅含子 effect 的行（作为 frame 容器）不会被 trigger，getter 不会执行。
            let newItemIndex: Atom<number>|undefined
            if (probe.deps.length > 0) {
                if (!addedAtomIndexesDep) {
                    source.addAtomIndexesDep()
                    addedAtomIndexesDep = true
                }
                newItemIndex = indexAtomFor(index)
            }
            const rowComputed: Computed = new Computed(() => {
                // CAUTION 特别注意这里面的变量，我们只希望 track 用户 mapFn 里面用到的外部 reactive 对象，不希望 track 到自己的 key/index。
                if (newItemIndex) {
                    const finalIndex = newItemIndex.raw
                    // CAUTION 位置契约守卫：newItemIndex 反映 source 的当前（终态）位置。
                    //  同一 digest 中行依赖可能先于本列表的结构 patch 被触发（batch 内
                    //  先写行依赖再 splice / 自定义调度器延迟 patch），此刻 list 还停留在
                    //  结构变更前的形态，按终态位置定点写会写错行。
                    const hasPendingStructuralInfos =
                        (list._triggerInfos !== undefined && list._triggerInfos.length > 0) ||
                        (list._inSession && list._sessionInfos !== undefined && list._sessionInfos.length > 0)
                    if (!hasPendingStructuralInfos) {
                        list.set(finalIndex, mapFn(source.data[finalIndex], newItemIndex, mapContext!))
                    } else {
                        // 行是否仍存活：终态 atomIndexes 里该位置的 atom 必须是自己
                        // （身份比较）。不是自己说明本行已被待重放的结构操作移除，
                        // 不能再用它的 item 运行 mapFn。
                        if (source.atomIndexes?.[finalIndex] !== newItemIndex) return
                        // 按行 effect frame 的身份定位行的当前（pre-patch）位置：
                        // 新值写在旧位置上，随后的结构重放会把它随行搬到终态位置。
                        const frames = list._effectFramesArray
                        const currentIndex = frames ? frames.findIndex(frame => frame !== undefined && frame.indexOf(rowComputed) !== -1) : -1
                        if (currentIndex !== -1) {
                            list.set(currentIndex, mapFn(source.data[finalIndex], newItemIndex, mapContext!))
                        }
                    }
                }
            }, undefined, true)
            probe.transferCapturesTo(rowComputed)
            // 探测已经完成了首次计算，行级 Computed 从 CLEAN 开始（下次依赖触发时正常重算）
            rowComputed._status = STATUS_CLEAN
            return [value, [rowComputed]]
        }

        // 全量重算是否是"重建"（首算之后的任何一次重算）：重建前要像删除行一样
        // 执行旧行的 cleanup（行级 effect 由 destroyChildren 统一销毁，但 cleanup
        // 槽位与 options.onCleanup 归本结构管理）。
        let hasComputedOnce = false

        return new RxList(
            function computation(this: RxList<U>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                if (hasComputedOnce) {
                    // 重建 = 所有旧行被替换：逐行执行 cleanup（与 splice 删除行的语义一致）
                    cleanupSlots?.forEach((slot) => { slot?.fn?.() })
                    if (options?.onCleanup) {
                        this.data.forEach((item) => options.onCleanup!(item))
                    }
                    // 旧行级 effect 已由 prepareTracking 的 destroyChildren 销毁，
                    // 记账数组必须清空重建，否则新列表更短时尾部残留悬垂 frame。
                    this.effectFramesArray!.length = 0
                }
                hasComputedOnce = true
                cleanupSlots = useContext ? [] : undefined

                const result: U[] = []
                const sourceData = source.data
                // 与 Array#map 一致：跳过稀疏洞，避免对洞位传 undefined index atom /
                // 把洞物化成显式 undefined 行（与增量 set 越界路径的稀疏结果分歧）。
                result.length = sourceData.length
                if (useContext) cleanupSlots!.length = sourceData.length
                this.effectFramesArray!.length = sourceData.length
                for (let i = 0; i < sourceData.length; i++) {
                    if (!Object.prototype.hasOwnProperty.call(sourceData, i)) continue

                    let mapContext: MapContext|undefined
                    if (useContext) {
                        const row = createRowContext()
                        cleanupSlots![i] = row.slot
                        mapContext = row.context
                    }

                    if (options?.skipItemEffect) {
                        result[i] = mapFn(sourceData[i], useIndex ? indexAtomFor(i) : source.atomIndexes?.[i]!, mapContext!)
                        // 共享的 frozen 空 frame：skipItemEffect 模式下每行必然无 effect，
                        // 不为对齐 splice 索引的占位再逐行分配空数组
                        this.effectFramesArray![i] = EMPTY_ITEM_FRAME
                    } else {
                        const [value, frame] = runItemAndCollectEffect(this, sourceData[i], i, mapContext)
                        result[i] = value
                        this.effectFramesArray![i] = frame
                    }
                }

                return result
            },
            function applyMapArrayPatch(this: RxList<U>, _data, triggerInfos) {
                // CAUTION 多 info 重放 × 行级 index atom 的位置契约：triggerInfo 的
                //  key/argv 是"操作时"的位置，而 source.atomIndexes 反映"终态"位置。
                //  单 info 时两者相同；同一次 digest 重放多条 info（batch/延迟调度）时，
                //  按操作时位置去取终态 atom 会拿到相邻行的 atom（行从此追踪错误位置）。
                //  数据本身的重放（值来自 argv/newValue，位置对本列表顺序重放）不受影响，
                //  因此只有真正使用 index atom 的 map 需要回退全量重算。
                if (triggerInfos.length > 1 && addedAtomIndexesDep) return false
                const hadIndexAtomsAtPatchStart = addedAtomIndexesDep

                for (const triggerInfo of triggerInfos) {

                    const { method , argv  ,key } = triggerInfo
                    if (__DEV__) {
                        assert((method === 'splice' || key !== undefined), 'trigger info has no method and key')
                        assert(triggerInfo.source === source, 'unexpected triggerInfo source')
                    }

                    options?.beforePatch?.(triggerInfo)

                    if (method === 'splice') {
                        // CAUTION 这里重新从已经改变的  source 去读，才能重新被 reactive proxy 处理，和全量计算时收到的参数一样
                        // CAUTION 第一次计算一定要使用 newItemsInArg 作为参数：有可能一批 triggerInfo 里
                        //  有多次元素位置变化，此时很难从 source.data 推断元素的新位置。
                        const newItemsInArgs = argv!.slice(2)
                        // CAUTION argv 按契约透传用户原始参数，start 可能是负数/越界/小数，
                        //  行 index（atomIndexes 下标）必须先归一化，否则负下标拿到 undefined
                        //  的 index atom，用户 mapFn 直接崩溃。归一化基准是本列表当前长度
                        //  （patch 逐条重放，此刻恰等于 source 在该次 splice 前的长度）。
                        const spliceStart = normalizeSpliceStart(argv![0], this.data.length)
                        const spliceDeleteCount = normalizeSpliceDeleteCount(argv![1], this.data.length, spliceStart)
                        const effectFrames: ReactiveEffect[][] = []
                        // CAUTION 与行严格等长的槽位数组（未注册的行留空槽），
                        //  否则"部分新行不注册 cleanup"会让整个数组错位。
                        const newSlots: MapCleanupSlot[] = []
                        const newItems = newItemsInArgs.map((newItemsInArg, index) => {
                            let mapContext: MapContext|undefined
                            if (useContext) {
                                const row = createRowContext()
                                newSlots[index] = row.slot
                                mapContext = row.context
                            }
                            let newItem: U
                            const newIndex = index + spliceStart
                            if (options?.skipItemEffect) {
                                newItem = mapFn(newItemsInArg, useIndex || addedAtomIndexesDep ? indexAtomFor(newIndex) : source.atomIndexes?.[newIndex]!, mapContext!)
                                effectFrames![index] = EMPTY_ITEM_FRAME
                            } else {
                                const [value, frame] = runItemAndCollectEffect(this, newItemsInArg, newIndex, mapContext)
                                newItem = value
                                effectFrames![index] = frame
                            }
                            return newItem!
                        })
                        const deletedItems = this.spliceArray(spliceStart, spliceDeleteCount, newItems)
                        const deletedFrames = spliceMany(this.effectFramesArray!, spliceStart, spliceDeleteCount, effectFrames)
                        deletedFrames.forEach((frame) => {
                            // CAUTION 稀疏行安全：越界 set（契约内透传）会让行记账产生洞，
                            //  reorder 的搬移又会把洞物化成显式 undefined（forEach 不再跳过）。
                            //  删除区间覆盖这类行时 frame 可能为 undefined。
                            frame?.forEach((effect) => {
                                this.destroyEffect(effect)
                            })
                        })
                        // 更新槽位数组并执行被删除行的 cleanup
                        if (useContext && cleanupSlots) {
                            // CAUTION 这里要把删除的行的 cleanup 都执行一遍
                            //  如果能从 return value 中进行销毁，应该使用 options.onCleanup 来注册一个统一的销毁函数，这样能提升性能。
                            const deletedSlots = spliceMany(cleanupSlots, spliceStart, spliceDeleteCount, newSlots)
                            deletedSlots.forEach((slot) => {
                                slot?.fn?.()
                            })
                        }
                        // 统一的销毁函数
                        if(options?.onCleanup) {
                            deletedItems.forEach((item) => {
                                options.onCleanup!(item)
                            })
                        }
                    } else if(method === 'reorder') {
                        // 数据、行 effect frame 和 cleanup 槽位必须按同一组 old→new
                        // 映射移动；旧实现只 reorder data，后续 set 会销毁错误行。
                        const order = argv![0]! as Order[]
                        const movedFrames = order.map(([oldIndex]) => this.effectFramesArray![oldIndex])
                        const movedSlots = cleanupSlots
                            ? order.map(([oldIndex]) => cleanupSlots![oldIndex])
                            : undefined
                        order.forEach(([, newIndex], index) => {
                            this.effectFramesArray![newIndex] = movedFrames[index]
                            if (cleanupSlots) cleanupSlots[newIndex] = movedSlots![index]
                        })
                        this.reorder(order, triggerInfo.reorderInfo as ReorderPatchInfo | undefined)
                    } else {
                        // explicit key change
                        // CAUTION 非稠密 key（负/小数）的 set 是数组属性赋值,不触及任何
                        //  行元素——按 key 物化会造出幽灵行（本列表写属性、filter 经
                        //  pending 插真实行）,忽略 ≡ 全量重算（2026-H3 round3 形态 fuzz 命中）。
                        if (!isDenseIndexKey(key)) continue
                        // CAUTION 新行的 item 必须取 info.newValue（操作时的值），不能读
                        //  source.data[key]：key 是"操作时"的位置，而 source.data 是重放时
                        //  的终态——同一次 digest 里 set 之后还有结构操作（batch 中
                        //  set+shift）时，终态位置上是别的元素甚至越界 undefined，
                        //  mapFn 会算出 NaN/undefined 且顺序永久错乱。
                        // 没有 method 说明是 explicit_key_change 变化
                        const index = key as number
                        const oldSlot = cleanupSlots?.[index]
                        let mapContext: MapContext|undefined
                        if (useContext) {
                            const row = createRowContext()
                            cleanupSlots![index] = row.slot
                            mapContext = row.context
                        }
                        const oldItem = this.data.at(index)!

                        // 与初始构建/splice 使用同一条行级依赖探测路径。旧实现只用
                        // collectEffect 收集子 effect，却没有把 mapFn 读取的 atom 依赖
                        // 转移到新的 rowComputed；set 后该行会永久失去响应。
                        const [newItem, newFrame] = runItemAndCollectEffect(
                            this,
                            triggerInfo.newValue as T,
                            index,
                            mapContext,
                        )
                        this.set(index, newItem)
                        this.effectFramesArray![index]?.forEach((effect) => {
                            this.destroyEffect(effect)
                        })
                        this.effectFramesArray![index] = newFrame

                        if (oldSlot?.fn) {
                            oldSlot.fn()
                        }
                        if(options?.onCleanup) {
                            options.onCleanup(oldItem)
                        }

                    }
                }
                // CAUTION 见函数开头的位置契约说明：若本轮重放中途第一次出现了
                //  需要 index atom 的行（addedAtomIndexesDep 由 false 变 true），
                //  这些行按操作时位置取的终态 atom 同样不可信，整体回退全量重算
                //  （full recompute 会先清理本轮已建的行再重建）。
                if (triggerInfos.length > 1 && !hadIndexAtomsAtPatchStart && addedAtomIndexesDep) return false
                options?.afterPatch?.(triggerInfos)
                if (__DEV__) {
                    // dev 不变量：行级记账结构必须与数据严格等长对齐。
                    // 错位意味着删除行时会销毁/执行到相邻行的 effect 或 cleanup。
                    assert(this.effectFramesArray!.length === this.data.length, 'map effectFramesArray misaligned with data')
                    if (cleanupSlots) {
                        assert(cleanupSlots.length === this.data.length, 'map cleanup slots misaligned with data')
                    }
                }
            },
            options?.scheduleRecompute,
            {
                onDestroy(this: RxList<U>)  {
                    itemProbe?.destroy()
                    itemProbe = undefined
                    if (addedAtomIndexesDep) {
                        source.removeAtomIndexesDep()
                    }
                    if (cleanupSlots) {
                        cleanupSlots.forEach((slot) => {
                            slot?.fn?.()
                        })
                    }
                    if(options?.onCleanup) {
                        this.data.forEach((item) => {
                            options.onCleanup!(item)
                        })
                    }
                }
            },
        )
    }
    // CAUTION reduce/reduceToAtom 的"纯尾插"增量判定必须以**该条 info 操作时**的
    //  源长度归一化 start（缺陷类：操作时位置 × 重放时终态）：多 info 重放时逐条
    //  拿终态长度回推会把"越界 clamp 到尾部"的 splice 误判为尾插（argv[0] 恰等于
    //  终态长 - 插入数），增量喂给 reduceFn 的 index 与全量重算分叉。单 info 时
    //  终态回推成立；多 info 经 digestReplay 内核取逐条操作时长度，判定不误入
    //  且真尾插序列（batch 内连续 push）保持增量。
    private static isPureTailAppend(info: TriggerInfo, lengthBefore: number): boolean {
        if (info.method !== 'splice') return false
        const deletedCount = (info.methodResult as unknown[] | undefined)?.length ?? 0
        if (deletedCount !== 0) return false
        return normalizeSpliceStart(info.argv![0], lengthBefore) === lengthBefore
    }
    reduce<U extends Computed = RxList<T>>(reduceFn: (last:U, item: T, index: number) => any, ResultComputed: new (...args:any[])=>U = RxList as any): U {
        const source = this
        return new ResultComputed(
            function computation(this: U) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                // 用 placeholder 生成一个新的 data。
                const placeholder = new ResultComputed()
                for(let i = 0; i < source.data.length; i++) {
                    const getFrame = ReactiveEffect.collectEffect!()
                    try {
                        reduceFn(placeholder, source.data[i], i)
                    } finally {
                        // 用户 reduceFn 抛错时也必须弹出全局 collect frame。
                        this.effectFramesArray![i] = getFrame() as ReactiveEffect[]
                    }
                }

                const result = placeholder.data
                placeholder.destroy()
                delete placeholder.data
                return result
            },
            function applyMapArrayPatch(this: U, _data:any, triggerInfos: TriggerInfo[]) {
                // 只有纯粹的尾部新增可以增量（判定与应用都按操作时长度，见 isPureTailAppend）
                const multi = triggerInfos.length > 1 ? reconstructDigestStates(source.data, triggerInfos) : null
                if (triggerInfos.length > 1 && !multi) return false
                const lengthBeforeAt = (infoIndex: number, info: TriggerInfo) => multi
                    ? multi.lengthBefore(infoIndex)
                    : source.data.length - (info.argv!.length - 2) + ((info.methodResult as unknown[] | undefined)?.length ?? 0)
                for (let i = 0; i < triggerInfos.length; i++) {
                    const info = triggerInfos[i]
                    if (info.method !== 'splice' || !RxList.isPureTailAppend(info, lengthBeforeAt(i, info))) return false
                }

                for (let infoIndex = 0; infoIndex < triggerInfos.length; infoIndex++) {
                    const triggerInfo = triggerInfos[infoIndex]
                    const { argv } = triggerInfo
                    // CAUTION 这里重新从已经改变的  source 去读，才能重新被 reactive proxy 处理，和全量计算时收到的参数一样
                    const newItemsInArgs = argv!.slice(2)
                    const originLength = lengthBeforeAt(infoIndex, triggerInfo)
                    for(let i = 0; i < newItemsInArgs.length; i++) {
                        const getFrame = ReactiveEffect.collectEffect!()
                        try {
                            reduceFn(this, newItemsInArgs[i], i + originLength)
                        } finally {
                            this.effectFramesArray![i + originLength] = getFrame() as ReactiveEffect[]
                        }
                    }
                }
            }
        )
    }
    reduceToAtom<U extends any>(reduceFn: (last:U, item: T, index: number) => any, initialValue: U): Atom<U> {
        const source = this
        return computed(
            function computation(this: Computed) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                return source.data.reduce(reduceFn, initialValue)
            },
            function applyMapArrayPatch(this: Computed, data:any, triggerInfos: TriggerInfo[]) {
                // 只有纯粹的尾部新增可以增量（判定与应用都按操作时长度，见 isPureTailAppend）
                const multi = triggerInfos.length > 1 ? reconstructDigestStates(source.data, triggerInfos) : null
                if (triggerInfos.length > 1 && !multi) return false
                const lengthBeforeAt = (infoIndex: number, info: TriggerInfo) => multi
                    ? multi.lengthBefore(infoIndex)
                    : source.data.length - (info.argv!.length - 2) + ((info.methodResult as unknown[] | undefined)?.length ?? 0)
                for (let i = 0; i < triggerInfos.length; i++) {
                    const info = triggerInfos[i]
                    if (info.method !== 'splice' || !RxList.isPureTailAppend(info, lengthBeforeAt(i, info))) return false
                }

                for (let infoIndex = 0; infoIndex < triggerInfos.length; infoIndex++) {
                    const triggerInfo = triggerInfos[infoIndex]
                    const { argv } = triggerInfo
                    // CAUTION 这里重新从已经改变的  source 去读，才能重新被 reactive proxy 处理，和全量计算时收到的参数一样
                    const newItemsInArgs = argv!.slice(2)
                    const originLength = lengthBeforeAt(infoIndex, triggerInfo)
                    for(let i = 0; i < newItemsInArgs.length; i++) {
                        data(reduceFn(data.raw, newItemsInArgs[i], i + originLength))
                    }
                }
            }
        )
    }

    find(matchFn:(item: T) => boolean): Atom<T | undefined> {
        const index = this.findIndex(matchFn)

        return computed(() => {
            const indexValue = index()
            return indexValue === -1 ? undefined : this.at(indexValue)
        }, undefined, true, {
            onDestroy() {
                destroyComputed(index)
            }
        })
    }
    findIndex(matchFn:(item: T) => boolean): Atom<number> {
        const source = this
        // 谓词是否读取过 reactive 数据：一旦为真，patch 入口一律回退全量重算。
        // CAUTION 2026-H2 mutation 债务清理删除了这里的逐项增量 cache
        //  （searchedItemAndIndexes/trackTargetToSearchItem/deleted 标记）：该 cache
        //  只被"元素内部 reactive 变化"的增量分支消费，而 cache 非空 ⟺ 谓词读过
        //  reactive 数据 ⟺ 入口的 hasItemReactiveDeps 门已经回退全量——分支构造性
        //  不可达（mutation 审计中 36 个 no-coverage mutant 的来源），无 reactive
        //  谓词的热路径还要为它每步搜索白付一次对象分配。
        //  行为契约不变：README「增量(响应式谓词→重算)」。
        let hasItemReactiveDeps = false

        // 运行一次谓词并登记其 reactive 读取；track 归属 resultComputed
        //（reactive 谓词的依赖订阅由此建立，变化时触发 patch → 全量回退）。
        // dataArr 是"该条 info 操作时"的源状态：单 info 时即 source.data，
        // 多 info 时来自 digestReplay 重建的快照（见 applyPatch）。
        function matchOne(dataArr: T[], current:number, resultComputed: Computed) {
            resultComputed.autoTrack()
            const getFrame = notifier.collectTrackTarget()
            let frameClosed = false
            let matchResult: boolean
            let trackTargets: any[]
            try {
                matchResult = matchFn(dataArr[current])
                trackTargets = getFrame()
                frameClosed = true
            } finally {
                // predicate 抛错时两个全局栈都必须恢复；否则 currentTrackFrame
                // 会永久强引用后续所有被 track 的对象。
                if (!frameClosed) getFrame()
                resultComputed.resetAutoTrack()
            }

            if (trackTargets!.length) hasItemReactiveDeps = true
            return matchResult!
        }

        function search(dataArr: T[], start:number, end: number, resultComputed: Computed) {
            for(let current=start; current < Math.min(end, dataArr.length);current++) {
                if (matchOne(dataArr, current, resultComputed)) return current
            }
            return -1
        }

        const result = computed<number>(
            function computation(this: Computed) {
                // 全量重算重新判定谓词是否 reactive（例如列表被替换成纯值行后回到增量热路径）
                hasItemReactiveDeps = false
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
                return search(source.data, 0, Infinity, this)
            },
            function applyPatch(this: Computed, data: Atom<number>, triggerInfos){
                // 增量 cache 无法逐 dep 精确退订/重订。只要 predicate 读取过
                // reactive 数据，就用 full recompute 保证每轮依赖集合与搜索区间一致；
                // 无 reactive predicate 的热路径仍保留增量 patch。
                if (hasItemReactiveDeps) return false
                // CAUTION 多 info 重放（缺陷类：操作时位置 × 重放终态，曾由 batchReplayFuzz
                //  动态命中）：splice 的负/越界 start 归一化与 match 扫描都必须发生在
                //  "该条 info 操作时"的源状态上。digestReplay 内核从终态逆推出逐条
                //  快照；不可重建（EKC 旧值 undefined 歧义等）回退全量重算。
                const multi = triggerInfos.length > 1 ? reconstructDigestStates(source.data, triggerInfos) : null
                if (triggerInfos.length > 1 && !multi) return false
                let patchSuccess = undefined
                // 每次 patch 都需要重新注册所有依赖。
                this.cleanup()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)

                // CAUTION 用 for...of 而不是 every：every 的回调若不显式 return true 会提前终止，
                //  导致一次 batch 里的多条 triggerInfo 只处理第一条。
                for (let infoIndex = 0; infoIndex < triggerInfos.length; infoIndex++) {
                    const triggerInfo = triggerInfos[infoIndex]
                    const stateNow = multi ? multi.after(infoIndex) : source.data
                    const { method , argv  ,key, source: triggerSource } = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    let startFindingIndex = Infinity
                    if (triggerSource === source ) {
                        if (method === 'reorder') {
                            // reorder 会同时改变候选索引和逐项 reactive cache。这里的增量
                            // 状态机无法仅靠 Symbol ITERATE_KEY 安全修补，回退全量重算。
                            patchSuccess = false
                            break
                        } else if (method === 'splice') {
                            // CAUTION argv 是用户原始参数（可能负/越界/小数），必须按 splice 发生时
                            //  的长度归一化成真实起点：负 start 直接参与会从 data[-1] 开始扫，
                            //  越界 start 会漏掉"append 产生新匹配"的场景。
                            const insertedCount = argv!.length - 2
                            const deletedCount = (triggerInfo.methodResult as unknown[] | undefined)?.length ?? 0
                            const lengthBeforeSplice = multi
                                ? multi.lengthBefore(infoIndex)
                                : source.data.length - insertedCount + deletedCount
                            const startIndex = normalizeSpliceStart(argv![0], lengthBeforeSplice)
                            // 可能新增了更小的能找到的，都从 startIndex 开始重新算。
                            if (this.data.raw == -1 || startIndex <= this.data.raw) {
                                startFindingIndex = startIndex
                            }

                        } else if (!isDenseIndexKey(key)) {
                            // 非稠密 key 的 set 是数组属性赋值,不触及任何元素:忽略。
                            // 不跳过的话,负 key 与现有 match 的比较/matchOne(dataArr[-1])
                            // 会把属性值当元素,可能把真实 match 覆写成 -1(幽灵 EKC 等价类)。
                        } else {
                            // explicit key change
                            if (this.data.raw === key) {
                                // 刚好把找到的弄没了
                                startFindingIndex = key as number
                            } else if(this.data.raw === -1 || (key as number) < this.data.raw) {
                                // 当前没有匹配（-1）时任何位置的变化都可能产生新匹配；
                                // 有匹配时只有更小的 index 才可能替换。快速验证这一个是不是新的 match。
                                if (matchOne(stateNow, key as number, this)) data(key as number)
                            }
                        }

                        // 需要从 startFindingIndex 开始重找，startFindingIndex 前面不需要
                        if (startFindingIndex !== Infinity) {
                            data(search(stateNow, startFindingIndex, Infinity, this))
                        }
                    } else {
                        // 非 source 的触发只可能来自谓词曾读取的 reactive 数据，该情形
                        // 已被入口的 hasItemReactiveDeps 全量回退拦截；防御性兜底回退。
                        patchSuccess = false
                        break
                    }
                }
                // 显式 return false 触发重算
                return patchSuccess
            },
            true
        )

        return result!
    }

    filter(filterFn: (item:T) => boolean): RxList<T> {
        const source = this
        const filtered = new RxList<T>([])
        // 初始全量构建阶段行按源顺序执行，直接 push 即可（O(1)）
        let initialBuildDone = false
        let mapList!: RxList<Atom<boolean>>

        // CAUTION 不变量：filtered 恒等于"mapList indicator 为 true 的行"按行序组成的
        //  子序列。所有定位都基于行（indicator atom 的身份/位置），绝不按值查找
        //  （indexOf / 值对齐扫描在重复原始值下会命中错误实例，破坏顺序）。
        //  - patch 中的 splice / set：beforePatch 基于"应用前"的 indicator 前缀计算
        //    受影响区间（removeStart/removeCount），新行首次运行只把匹配项按序上报到
        //    pending.inserts，flushPending 用一次 spliceArray 精确应用差量；
        //  - 已有行 toggle：以自身 indicator atom 在 mapList 中定位（身份比较），
        //    位置 = 前缀中为 true 的行数；
        //  - reorder：行序整体变化，用最终 indicator 顺序全量重建一次。
        let pending: {removeStart: number, removeCount: number, inserts: T[]} | null = null
        let rebuildAfterPatch = false

        const flushPending = () => {
            if (!pending) return
            const {removeStart, removeCount, inserts} = pending
            pending = null
            if (removeCount > 0 || inserts.length > 0) {
                filtered.spliceArray(removeStart, removeCount, inserts)
            }
        }

        const rebuildFromIndicators = () => {
            const next: T[] = []
            const rows = mapList.data
            for (let i = 0; i < rows.length; i++) {
                if (rows[i]?.raw) next.push(source.data[i])
            }
            filtered.spliceArray(0, filtered.data.length, next)
        }

        // 行在 filtered 中的位置：以 indicator atom 身份定位到行，位置 = 前缀中
        // raw 为 true 的行数。toggle 发生在 patch 之外，mapList 与 filtered 一致。
        const locateRowPosition = (self: Atom<boolean>) => {
            const rows = mapList.data
            let pos = 0
            for (let i = 0; i < rows.length; i++) {
                if (rows[i] === self) return pos
                if (rows[i]?.raw) pos++
            }
            return -1
        }

        // CAUTION mapList 全量重算(初次构建之外:patch 抛错后的错误恢复、行升级为
        //  响应式后的多 info 回退)必须整体重建 filtered:
        //  1. 抛错的 patch 轮可能留下未 flush 的 pending——新行的首跑上报会被引流进
        //     这个死 pending(afterPatch 不会再来),filtered 永久缺行;
        //  2. 全量重算的行 getter 首跑走"push 追加"路径,不清空旧内容会双倍计数。
        //  错误恢复语义(README §5/方法 12 缺陷类 4)要求恢复后 ≡ 终态全量重算,
        //  这里在 fullRecompute 见证事件里复位增量状态并清空 filtered,
        //  行 getter 首跑按源序重新填充——与初次构建同一路径。
        //  (2026-H3 round3 形态操作 fuzz 的错误注入探针动态命中。)
        mapList = this.map((item) => {
            return computed(({lastValue} ) => {
                const matched = filterFn(item)
                // AtomComputed 初始值为 null：首次运行时 raw 还不是 boolean
                const isFirstRun = typeof lastValue.raw !== 'boolean'
                if (isFirstRun) {
                    if (matched) {
                        if (!initialBuildDone) {
                            // 全量构建按源顺序逐行执行，尾插即为正确位置
                            filtered.push(item)
                        } else if (pending) {
                            // patch 中的新行按序上报，由 flushPending 统一定位应用
                            pending.inserts.push(item)
                        } else {
                            // rebuildAfterPatch 流程（reorder 批次中的新行）：先尾插，
                            // afterPatch 以最终 indicator 顺序重建
                            filtered.push(item)
                        }
                    }
                } else if (matched !== lastValue.raw) {
                    const pos = locateRowPosition(lastValue as Atom<boolean>)
                    if (pos !== -1) {
                        if (matched) {
                            filtered.splice(pos, 0, item)
                        } else {
                            filtered.splice(pos, 1)
                        }
                    } else if (!matched) {
                        // 行已不在 mapList 中（理论不可达），按值兜底移除
                        const index = filtered.data.indexOf(item)
                        if (index !== -1) filtered.splice(index, 1)
                    } else {
                        filtered.push(item)
                    }
                }
                return matched
            }, undefined, true)
        }, {
            ignoreIndex: true,
            beforePatch(info) {
                // 应用上一条 info 计算出的差量（此刻 mapList 已应用上一条，
                // filtered 补齐后两者重新一致，才能为本条计算前缀）
                flushPending()
                if (info.method === 'reorder') rebuildAfterPatch = true
                // 一旦进入重建流程，后续 info 不再做差量定位，统一在 afterPatch 重建
                if (rebuildAfterPatch) return

                const rows = mapList.data
                if (info.method === 'splice') {
                    const deletedCount = (info.methodResult as unknown[] | undefined)?.length ?? 0
                    // patch 逐条重放：mapList 尚未应用本条 info，长度即 splice 前的长度
                    const start = normalizeSpliceStart(info.argv![0], rows.length)
                    let removeStart = 0
                    let removeCount = 0
                    const bound = Math.min(start + deletedCount, rows.length)
                    // rows[i]?.raw：越界 set（契约内透传）会让行数组出现洞/显式 undefined
                    for (let i = 0; i < bound; i++) {
                        if (rows[i]?.raw) {
                            if (i < start) removeStart++
                            else removeCount++
                        }
                    }
                    pending = {removeStart, removeCount, inserts: []}
                } else {
                    // explicit key change（set）：同位置替换
                    const key = info.key as number
                    let removeStart = 0
                    const bound = Math.min(key, rows.length)
                    for (let i = 0; i < bound; i++) {
                        if (rows[i]?.raw) removeStart++
                    }
                    pending = {removeStart, removeCount: rows[key]?.raw ? 1 : 0, inserts: []}
                }
            },
            afterPatch() {
                flushPending()
                if (rebuildAfterPatch) {
                    rebuildFromIndicators()
                    rebuildAfterPatch = false
                }
            }
        })
        mapList.on('fullRecompute', () => {
            // 只在重建(非首次构建)时需要:首次构建 filtered 本来就是空的,复位幂等
            pending = null
            rebuildAfterPatch = false
            if (filtered.data.length) filtered.splice(0, Infinity)
        })
        initialBuildDone = true

        filtered.on('destroy', () => mapList.destroy())

        return filtered
    }
    every(fn: (item:T) => boolean): Atom<boolean> {
        const some = this.some((item) => !fn(item))
        return computed(() => {
            return !some()
        }, undefined, true, {
            onDestroy() {
                destroyComputed(some)
            }
        })
    }
    some(fn: (item:T) => boolean) : Atom<boolean>{
        const index = this.findIndex(fn)
        return computed(() => {
            return index() != -1
        }, undefined, true, {
            onDestroy() {
                destroyComputed(index)
            }
        })
    }
    groupBy<K>(getKey: (item: T) => K) {
        const source = this
        return new RxMap<K, RxList<T>>(
            function computation(this: RxMap<any, RxList<T>>) {
                const groups = new Map()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                for (let i = 0; i < source.data.length; i++) {
                    const item = source.data[i]
                    const key = getKey(item)
                    if (!groups.has(key)) {
                        groups.set(key, new RxList([]))
                    }
                    groups.get(key)!.push(item)
                }
                return groups
            },
            function applyPatch(this: RxMap<any, RxList<T>>, _data, triggerInfos) {
                // CAUTION 多 info 重放：insertInSourceOrder/removeAtSourcePosition 的
                //  组内定位依赖"前缀 [0, index) 未被本次触及"，前缀必须按**该条 info
                //  操作时**的源状态计数——重放时 source.data 已是终态。digestReplay
                //  内核从终态逆推出每条 info 应用后的快照（stateNow），逐条与单 info
                //  语义对齐；不可重建（EKC 旧值 undefined 歧义等）回退全量重算。
                const multi = triggerInfos.length > 1 ? reconstructDigestStates(source.data, triggerInfos) : null
                if (triggerInfos.length > 1 && !multi) return false
                let stateNow: T[] = source.data
                const sameKey = (a: any, b: any) => a === b || (a !== a && b !== b)
                const insertInSourceOrder = (item: T, sourceIndex: number) => {
                    const groupKey = getKey(item)
                    if (!this.data.has(groupKey)) {
                        this.set(groupKey, new RxList([]))
                    }
                    let groupIndex = 0
                    for (let i = 0; i < sourceIndex; i++) {
                        if (sameKey(getKey(stateNow[i]), groupKey)) groupIndex++
                    }
                    this.data.get(groupKey)!.splice(groupIndex, 0, item)
                }

                // CAUTION 删除项在组内的定位必须按位置计数，不能用 indexOf：
                //  重复原始值下 indexOf 会命中错误实例，破坏组内顺序。
                //  被删项的组内位置 = splice/set 起点之前（前缀未被本次触及）的同 key
                //  元素数；同一次 splice 中更早的同 key 删除项此刻已被移除，不参与计数。
                const removeAtSourcePosition = (item: T, sourceIndex: number) => {
                    const groupKey = getKey(item)
                    const group = this.data.get(groupKey)
                    if (!group) return
                    let pos = 0
                    const bound = Math.min(sourceIndex, stateNow.length)
                    for (let i = 0; i < bound; i++) {
                        if (sameKey(getKey(stateNow[i]), groupKey)) pos++
                    }
                    if (pos < group.data.length) group.splice(pos, 1)
                    // 空组必须删键：全量 computation 不会保留空组，增量路径若只
                    // 清内容不删键，has/size/keys 与全量分叉，空 RxList 子结构泄漏。
                    if (group.data.length === 0) {
                        this.delete(groupKey)
                        group.destroy()
                    }
                }

                for (let infoIndex = 0; infoIndex < triggerInfos.length; infoIndex++) {
                    const triggerInfo = triggerInfos[infoIndex]
                    stateNow = multi ? multi.after(infoIndex) : source.data
                    const { method , argv  ,key, oldValue, newValue, methodResult, type} = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    if (method === 'splice') {
                        const deleteItems = methodResult as T[] || []
                        const newItemsInArgs = argv!.slice(2)
                        const lengthBeforeSplice = multi
                            ? multi.lengthBefore(infoIndex)
                            : source.data.length - newItemsInArgs.length + deleteItems.length
                        const startIndex = normalizeSpliceStart(argv![0], lengthBeforeSplice)
                        deleteItems.forEach((item) => {
                            // 所有删除项都从 startIndex 起：前缀 [0, startIndex) 未被本次触及
                            removeAtSourcePosition(item, startIndex)
                        })
                        newItemsInArgs.forEach((item, index) => {
                            insertInSourceOrder(item, startIndex + index)
                        })

                    } else if (method === 'reorder') {
                        // membership 不变，只按该 info 操作时的 source 顺序重排现有 group，
                        // 保持 group RxList 引用稳定。
                        // CAUTION 全下标扫描而不是 Array#filter：全量 computation 按
                        //  [0, length) 读取（洞位读出 undefined 参与分组），filter 会跳洞
                        //  ——稀疏源上 reorder 会把"undefined 组"清空而键残留，与全量
                        //  重算分叉（洞的物化语义必须两侧一致，同 map/indexBy 先例）。
                        this.data.forEach((group, groupKey) => {
                            const nextItems: T[] = []
                            for (let i = 0; i < stateNow.length; i++) {
                                if (sameKey(getKey(stateNow[i]), groupKey)) nextItems.push(stateNow[i])
                            }
                            group.spliceArray(0, group.data.length, nextItems)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // 非稠密 key 的 set 是数组属性赋值,不触及任何元素:忽略 ≡ 全量重算
                        // (幽灵 EKC 会把属性赋值物化成幽灵组成员,2026-H3 round3 形态 fuzz 等价类)
                        if (!isDenseIndexKey(key)) continue
                        // explicit key change：set 不触及 [0, key) 前缀
                        removeAtSourcePosition(oldValue as T, key as number)
                        insertInSourceOrder(newValue as T, key as number)
                    }
                }
            }
        )
    }

    // CAUTION keyof NonNullable<T> 而不是 keyof T：computation/patch 都显式支持
    //  null/undefined 行（跳过），keyof (X | null) 是 never 会把属性形式对可空
    //  行列表整个封死；对非空 T 两者相同（纯放宽，无下游破坏）。
    indexBy(inputIndexKey: keyof NonNullable<T>|((item: T) => any)) {
        const source = this
        // CAUTION 稀疏行安全(2026-H2 缺陷类:OOB set × 属性形式 indexBy):越界 set
        //  产生的洞位读出 undefined——属性读 `(undefined)[key]` 直接 TypeError 且派生
        //  链永久毒化,违反 sparseSetOperatorsSweep 的"不崩溃且可恢复"等价类。
        //  洞位行(undefined)一律跳过:全量侧按 hasOwnProperty 跳洞(与 map 一致),
        //  patch 侧删除/替换遇 undefined 旧值视为"无旧 entry"。
        return new RxMap<any, T>(
            function computation(this: RxMap<any, T>) {
                const map = new Map()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                for (let i = 0; i < source.data.length; i++) {
                    const item = source.data[i]
                    // 洞位与显式 null/undefined 行统一忽略(无法取 key)
                    if (item == null) continue
                    const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(item) : item[inputIndexKey]
                    assert(!map.has(indexKey), 'indexBy key is already exist')
                    map.set(indexKey, item)
                }
                return map
            },
            function applyPatch(this: RxMap<any, T>, _data, triggerInfos) {
                triggerInfos.forEach((triggerInfo) => {
                    const { method , argv  ,key, oldValue, newValue, methodResult, type} = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    if (method === 'splice') {
                        const deleteItems = methodResult as T[] || []
                        deleteItems.forEach((item) => {
                            if (item == null) return // 稀疏洞/null 行:无 entry 可删
                            const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(item) : item[inputIndexKey]
                            this.delete(indexKey)
                        })
                        const newItemsInArgs = argv!.slice(2)
                        newItemsInArgs.forEach((item) => {
                            // CAUTION 与全量 computation 的 null/undefined 行跳过语义对称：
                            //  插入侧漏守卫时 push(null) 的属性读直接 TypeError 抛给
                            //  变更调用方（2026-H3 round4 动态复现，与删除侧守卫同一等价类）。
                            if (item == null) return
                            const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(item) : item[inputIndexKey]

                            assert(!this.data.has(indexKey), 'indexBy key is already exist')
                            this.set(indexKey, item)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // 非稠密 key:数组属性赋值,无元素变化(幽灵 EKC 等价类,忽略 ≡ 全量)
                        if (!isDenseIndexKey(key)) return
                        // explicit key change(OOB set 的 oldValue 为 undefined:无旧 entry;
                        // null 旧行与全量语义一致地视为"无 entry")
                        if (oldValue != null) {
                            const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(oldValue as T) : (oldValue as NonNullable<T>)[inputIndexKey]
                            this.delete(indexKey)
                        }
                        // set(i, null/undefined)：全量语义跳过该行，无新 entry
                        if (newValue != null) {
                            const newKey = typeof inputIndexKey === 'function' ? inputIndexKey(newValue as T) : (newValue as NonNullable<T>)[inputIndexKey]
                            this.set(newKey, newValue as T)
                        }
                    }
                    // 还有可能是 reorder, reorder 对 map 来说没有影响。
                })
            }
        )
    }
    toArray() {
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
        return this.data
    }
    toMap() {
        const source = this
        // CAUTION 稀疏行安全(与 indexBy 同一缺陷类):洞位/显式 undefined 行解构
        //  `const [k,v] = undefined` 直接 TypeError 且派生链永久毒化。undefined 行
        //  统一忽略(全量与 patch 两侧一致),保持"OOB set 不崩溃且可恢复"等价类。
        return new RxMap<T extends [any, any] ? T[0] : any, T extends [any, any] ? T[1] : any>(
            function computation(this: RxMap<any, T>) {
                const map = new Map()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                for (let i = 0; i < source.data.length; i++) {
                    const entry = source.data[i] as [any, any] | undefined
                    if (entry === undefined) continue
                    const [key, value] = entry
                    assert(!map.has(key), 'indexBy key is already exist')
                    map.set(key, value)
                }
                return map
            },
            function applyPatch(this: RxMap<any, T>, _data, triggerInfos) {
                triggerInfos.forEach((triggerInfo) => {
                    const { method , argv  ,key, oldValue, newValue, methodResult, type} = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    if (method === 'splice') {
                        const deleteItems = methodResult as ([any, any] | undefined)[] || []
                        deleteItems.forEach((entry) => {
                            if (entry === undefined) return
                            this.delete(entry[0])
                        })
                        const newItemsInArgs = argv!.slice(2) as ([any, any] | undefined)[]
                        newItemsInArgs.forEach((entry) => {
                            // CAUTION 与全量 computation 的 undefined 行跳过语义对称：
                            //  插入侧直接解构时 push(undefined) 当场 TypeError 抛给变更
                            //  调用方（2026-H3 round4 动态复现，与删除侧守卫同一等价类）。
                            if (entry === undefined) return
                            this.set(entry[0], entry[1])
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // 非稠密 key:数组属性赋值,无元素变化(幽灵 EKC 等价类,忽略 ≡ 全量)
                        if (!isDenseIndexKey(key)) return
                        // explicit key change(OOB set 的 oldValue 为 undefined:无旧 entry)
                        if (oldValue !== undefined) {
                            this.delete((oldValue as [any, any])[0])
                        }
                        // set(i, undefined)：全量语义跳过该行，无新 entry
                        if (newValue !== undefined) {
                            const [newKey, newItem] = newValue as [any, any]
                            this.set(newKey, newItem)
                        }
                    }
                    // 还有可能是 reorder, reorder 对 map 来说没有影响。
                })
            }
        )
    }
    toSet(): RxSet<T> {
        const base = this
        return new RxSet<T>(
            function computation(this: RxSet<T>) {
                this.manualTrack(base, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(base, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                return new Set(base.data)
            },
            function applyPatch(this: RxSet<T>, _data, triggerInfos) {
                triggerInfos.forEach((triggerInfo) => {
                    const { method , argv  ,key, oldValue, newValue, methodResult, type} = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    if (method === 'splice') {
                        const deleteItems = methodResult as T[] || []
                        deleteItems.forEach((item) => {
                            if (!base.data.includes(item)) this.delete(item)
                        })
                        const newItemsInArgs = argv!.slice(2)
                        newItemsInArgs.forEach((item) => {
                            this.add(item)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // 非稠密 key:数组属性赋值,无成员变化(幽灵 EKC 等价类,忽略 ≡ 全量)
                        if (!isDenseIndexKey(triggerInfo.key)) return
                        // explicit key change
                        if (!base.data.includes(oldValue as T)) this.delete(oldValue as T)
                        this.add(newValue as T)
                    }
                    // 还有可能是 reorder, reorder 对 set 来说没有影响。
                })
            }
        )
    }
    // CAUTION length 惰性创建：每个 RxList（包括所有派生列表）无条件预建一个
    //  length computed 曾是主要的固定开销之一。惰性创建以 createDetached 包裹，
    //  解决旧注释里"在 autorun/computed 中读会被当作 children 误销毁"的问题。
    declare _length?: Atom<number>
    get length(): Atom<number> {
        return this._length ?? (this._length = ReactiveEffect.createDetached(() => {
            const source = this
            const length = computed(
                function computation(this: Computed) {
                    this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                    return source.data!.length
                },
                function applyPatch(this: Computed, data: Atom<number>){
                    data(source.data.length)
                }
            )
            setComputedRetainedDiagnosticSource(length, 'RxList.length')
            return length
        }))
    }

    // indexKeyDeps 的退订清扫：Notifier 不会在 effect 退订时回调 RxList，
    // 因此改为在结构变更入口 pruneIndexKeyDeps 惰性删除空 dep（见该方法注释）。
    // onUntrack 钩子保留为空，以兼容可能的外部调用。
    /**
     * @internal
     */
    onUntrack(_effect: ReactiveEffect) {

    }
    /**
     * @internal
     * 统一资源清理钩子（见 ReactiveEffect.destroyResources）：实例 destroy()、
     * destroyChildren、destroyComputed 三个入口都恰好执行一次。
     */
    destroyResources() {
        // CAUTION 用 _length 判断：length 是惰性 getter，直接访问会先创建再销毁
        if (this._length) destroyComputed(this._length)
        this._effectFramesArray?.forEach((frames) => {
          // 稀疏行安全：越界 set + reorder 会在记账数组中留下显式 undefined 项
          frames?.forEach((frame) => {
            this.destroyEffect(frame)
          })
        })
        this._indexKeyDeps?.clear()
        this.atomIndexes = undefined
        super.destroyResources()
    }

    createSelection(currentValues: RxSet<T|number>|Atom<T|null|number>, autoResetValue?: boolean) {
        return createSelection(this, currentValues, autoResetValue)
    }
    createSelections(...args: [RxSet<T|number>|Atom<T|null|number>, boolean?][]) {
        return createSelections<T>(this, ...args)
    }
    createIndexKeySelection(currentValues: RxSet<number>|Atom<null|number>, autoResetValue?:boolean) {
        return createIndexKeySelection(this, currentValues, autoResetValue)
    }

    public concat(...others: RxList<T>[]): RxList<T> {
        const sources = [this, ...others]
        return new RxList<T>(
            function computation(this: RxList<T>) {
                // Track each source for incremental updates
                sources.forEach(src => {
                    this.manualTrack(src, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                    this.manualTrack(src, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
                })

                // Full initial merge（逐项 push，规避大数组 spread 的实参上限）
                const merged: T[] = []
                sources.forEach(src => {
                    for (const item of src.data) {
                        merged.push(item)
                    }
                })
                return merged
            },
            function applyPatch(this: RxList<T>, _data, triggerInfos) {
                // 多源 batch 的 offset 取决于每条 patch 的中间长度；无法从最终
                // source 快照无歧义还原时直接全量重算。
                if (triggerInfos.length !== 1) return false
                // Figure out which source changed, then incrementally update
                for (const info of triggerInfos) {
                    const sourceIndex = sources.indexOf(info.source as RxList<T>)
                    if (sourceIndex === -1) {
                        return false
                    }

                    // Calculate offset of that source in the final array
                    let offset = 0
                    for (let i = 0; i < sourceIndex; i++) {
                        offset += sources[i].data.length
                    }

                    const { method, argv, newValue, methodResult, key } = info
                    if (method === 'splice') {
                        const deletedItems = (methodResult as T[]) || []
                        const newItems = argv!.slice(2) as T[]
                        const lengthBeforeSplice = sources[sourceIndex].data.length - newItems.length + deletedItems.length
                        const spliceStart = normalizeSpliceStart(argv![0], lengthBeforeSplice)
                        // 按变更源的 segment 位置操作，不能用全局 indexOf：跨源重复值
                        // 会删除前一个 source 中的同值元素。
                        this.spliceArray(offset + spliceStart, deletedItems.length, newItems)
                    } else if (method === 'reorder') {
                        return false
                    } else if (typeof key === 'number' && Number.isInteger(key) && key >= 0) {
                        // CAUTION 段长守卫：越界 set（契约内透传）会让源段长度跳变
                        //  （len 3 → set(10) → len 11），EKC 的 key 落在旧段之外时按
                        //  段内偏移直写会覆盖到后续源的段（B 段元素整体错位，结构性
                        //  错乱而不只是洞物化差异）。回退全量重算与终态源对齐。
                        //  旧段长 = 本列表长 − 其他源现长（单 info 守卫已保证其他源
                        //  在本次 digest 未变）。
                        let othersLength = 0
                        for (let i = 0; i < sources.length; i++) {
                            if (i !== sourceIndex) othersLength += sources[i].data.length
                        }
                        if (key >= this.data.length - othersLength) return false
                        this.set(offset + key, newValue as T)
                    } else {
                        return false
                    }
                }
            }
        )
    }

    public slice(start?: number, end?: number): RxList<T> {
        const source = this
        // 与 Array#slice 一致，先执行 ToIntegerOrInfinity；后续区间 patch
        // 只能使用整数边界，否则小数参与差量计算会产生半索引。
        start = toIntegerOrInfinity(start ?? 0)
        end = toIntegerOrInfinity(end ?? Infinity)
        
        /** Utility: clamp the user-provided slice range. */
        const clampIndexes = (length: number) : [number,number]|undefined =>  {
            if (start >= length) return undefined
            if (end < -(length)) return undefined
            // mimic standard JS slice behavior
            const s = start! < 0 ? Math.max(0, length + start!) : Math.min(start!, length)
            const e = end! < 0 ? Math.max(0, length + end!) : Math.min(end!, length)
            return s >= e ? undefined : [s, e]
        }

        let lastIndexes: [number, number]|undefined = undefined
        return new RxList<T>(
            /** 1) Full Recompute: just slice right now. */
            function computation(this: RxList<T>) {
                // track changes from source
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)

                const idxs = clampIndexes(source.data.length)
                lastIndexes = idxs
                return idxs ? source.data.slice(idxs[0], idxs[1]) : []
            },

            /** 2) Incremental Patch: interpret splices/sets to update our slice accordingly. */
            function applyPatch(this: RxList<T>, _data, triggerInfos) {
                // CAUTION 多 info 重放（缺陷类：操作时位置 × 重放终态）：区间差量以
                //  info 的操作时位置做区间算术，补元素也必须取自**该条 info 操作时**
                //  的源状态。digestReplay 内核从终态逆推出逐条快照（stateNow），
                //  与单 info 语义逐条对齐；不可重建时回退全量重算。
                const multi = triggerInfos.length > 1 ? reconstructDigestStates(source.data, triggerInfos) : null
                if (triggerInfos.length > 1 && !multi) return false
                for (let infoIndex = 0; infoIndex < triggerInfos.length; infoIndex++) {
                    const info = triggerInfos[infoIndex]
                    const stateNow = multi ? multi.after(infoIndex) : source.data
                    // ensure it's from this source
                    if (info.source !== source) return false
                    // reorder（sortSelf/reposition/swap）会改变区间内元素的相对顺序，
                    // 无法用区间差量表达，直接全量重算。
                    if (info.method === 'reorder') return false
                    // 负 slice 边界会随 source.length 变化整体平移；现有绝对区间
                    // patch 无法安全表达，回退 full computation。
                    if (info.method === 'splice' && (start! < 0 || end! < 0)) return false
                    const idxs = clampIndexes(stateNow.length)
                    // 现在不合法了，清空
                    if (!idxs || !lastIndexes) {
                        return false
                    }

                    // 原来和现在都合法
                    const { method, argv, newValue, methodResult: deletedItems, key } = info

                    if (method === 'splice') {
                        const insertedItems = argv!.slice(2) as T[]
                        // CAUTION 必须 continue 而不是 return：多 info 重放中 return 会
                        //  静默丢弃后续 info（单 info 时代的遗留写法，multi 下是缺陷）。
                        if (deletedItems.length === 0 && insertedItems.length === 0) continue

                        const lastSourceLength = multi
                            ? multi.lengthBefore(infoIndex)
                            : source.data.length - insertedItems.length + deletedItems.length
                        const spliceStart = normalizeSpliceStart(argv![0], lastSourceLength)
                        const spliceEffectEnd = spliceStart + deletedItems.length
                        const lengthChange = insertedItems.length  - deletedItems.length


                        const ucHead = lastIndexes[0] < spliceStart ? [lastIndexes[0], Math.min(spliceStart, lastIndexes[1])] : undefined
                        const ucTail = lastIndexes[1] > spliceEffectEnd ? [Math.max(spliceEffectEnd, lastIndexes[0])+lengthChange, lastIndexes[1]+lengthChange] : undefined
                        const ucTailOldIndex = ucTail ? Math.max(spliceEffectEnd, lastIndexes[0]) - lastIndexes[0]: undefined

                        if (!ucHead && !ucTail) {
                            return false
                        }
                        // 如果影响了原序列，并且影响范围在有效范围内，就要先处理原序列
                        // 已经找到了老的序列的新 index，如何进行更新策略？
                        if ((ucHead || ucTail) && !(spliceStart > idxs[1] || spliceEffectEnd < idxs[0])) {
                            // 1.如果 splice 影响的是中间，先把中间处理了，并且仍然在有效范围内。才有处理的必要
                            if (ucHead && ucTail) {
                                // CAUTION spliceArray 而不是 spread：中间段可与源 splice 的
                                //  插入量同量级（十万行级），spread 实参直接 RangeError
                                //  （spliceMany 存在的同一动机；2026-H3 round4 动态复现）。
                                this.spliceArray(ucHead[1]-ucHead[0], ucTailOldIndex! - (ucHead[1]-ucHead[0]), stateNow.slice(ucHead[1], ucTail[0]))
                            } else if (ucHead) {
                                this.splice(ucHead[1]-ucHead[0], Infinity)
                            } else {
                                this.splice(0, ucTailOldIndex!)
                            }
                        }

                        const oldStart = ucHead ? ucHead[0] : (ucTail ? ucTail[0] : undefined)
                        const oldEnd = ucTail ? ucTail[1] : (ucHead ? ucHead[1] : undefined)
                        if (oldStart! > idxs[1] || oldEnd! < idxs[0]) {
                            return false
                        }

                        if(oldStart! > idxs[0]) {
                            this.spliceArray(0, 0, stateNow.slice(idxs[0], oldStart))
                        } else if(oldStart! < idxs[0]) {
                            this.splice(0, idxs[0]-oldStart!)
                        }

                        if (oldEnd! < idxs[1]) {
                            this.spliceArray(this.data.length, 0, stateNow.slice(oldEnd, idxs[1]))
                        } else if(oldEnd! > idxs[1]) {
                            this.splice(idxs[1] - idxs[0], Infinity)
                        }

                        lastIndexes = idxs
                    } else {
                        // 非稠密 key(负/小数)是数组属性赋值:不触及区间内任何元素。
                        // 小数 key 恰落在区间内时,不跳过会经 splice 归一化替换掉真实行
                        // (幽灵替换,2026-H3 round3 形态 fuzz 等价类)。
                        if (!isDenseIndexKey(key)) continue
                        // explicit key change or "set"
                        // remove oldValue if it was in our slice
                        const index = key as number
                        if ( !(index < lastIndexes[0]||index >= lastIndexes[1])) {
                            this.splice(index - lastIndexes[0], 1, newValue as T)
                        }

                    }
                }
            }
        )
    }
}

type SelectionInner = {
    trackIndicators:any,
    trackCurrentValues:any,
    createNewIndicator:any,
    updateIndicatorsFromCurrentValueChange:any,
    stopAutoResetValue:any,
    deleteIndicator:any,
    resetIndicators:any,
    currentValues:any
}
export function createSelectionInner<T>(source: RxList<T>, currentValues: RxSet<T|number>|Atom<T|null|number>, autoResetValue = false): SelectionInner {
    function trackCurrentValues(list: Computed) {
        if (isAtom(currentValues)) {
            list.manualTrack(currentValues, TrackOpTypes.ATOM, 'value');
        } else {
            list.manualTrack(currentValues, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
        }
    }

    function trackIndicators(list: Computed) {
        list.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
        list.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
    }

    // CAUTION indicator 记账必须支持重复 item（同值/同引用占多行）：
    //  旧实现 Map<item, indicator> 在重复行下后写覆盖前写——currentValues 变化只更新
    //  最后一行（其余行永不更新），删除任一行还会把存活孪生行的条目一并误删
    //  （该行 indicator 永久失联，可观察为反选后卡 true）。
    //  存储用 CompactDep 同款紧凑手法：唯一 item（常见场景，axii 大列表按行选中）
    //  直接存 indicator 本身——零额外分配，与修复前同等内存；出现孪生行才升级 Set。
    //  indicator 是 atom（函数），与 Set 用 instanceof 可靠区分。
    const itemToIndicators: Map<any, Atom<boolean> | Set<Atom<boolean>>> = new Map()

    function createNewIndicator(item:T) {
        const indicator = atom(isAtom(currentValues) ? currentValues.raw === item : currentValues.data.has(item))
        const existing = itemToIndicators.get(item)
        if (existing === undefined) {
            itemToIndicators.set(item, indicator)
        } else if (existing instanceof Set) {
            existing.add(indicator)
        } else if (existing !== indicator) {
            itemToIndicators.set(item, new Set([existing, indicator]))
        }
        return indicator
    }

    // 存活检查的批量优化：单条删除用 includes（零分配），批量删除由调用方传入
    // 预构建的 survivors Set，避免 O(删除数 × 列表长)。
    function deleteCurrentValueIfItemRemoved(item:T, survivors?: Set<T>) {
        // 重复 item：仍有同 item 行存活时不回收选中值，否则存活行会被误反选。
        // （includes/Set.has 都是 SameValueZero，与 Map/Set 成员语义一致，NaN 亦可命中。）
        if (survivors ? survivors.has(item) : source.data.includes(item)) return
        if (isAtom(currentValues)) {
            if (item === currentValues.raw) {
                currentValues(null)
            }
        } else {
            if(currentValues.data.has(item)) {
                currentValues.delete(item)
            }
        }
    }

    function deleteIndicator(item:T, indicator?: Atom<boolean>) {
        const existing = itemToIndicators.get(item)
        if (existing === undefined) return
        if (indicator === undefined) {
            itemToIndicators.delete(item)
            return
        }
        if (existing instanceof Set) {
            existing.delete(indicator)
            if (existing.size === 1) {
                // 降级回紧凑存储（与 CompactDep 的 overflow 收缩一致）
                const [only] = existing
                itemToIndicators.set(item, only)
            } else if (existing.size === 0) {
                itemToIndicators.delete(item)
            }
        } else if (existing === indicator) {
            itemToIndicators.delete(item)
        }
    }

    function updateIndicators(item: T, value: boolean) {
        const existing = itemToIndicators.get(item)
        if (existing === undefined) return
        if (existing instanceof Set) {
            existing.forEach(indicator => indicator(value))
        } else {
            existing(value)
        }
    }

    function updateIndicatorsFromCurrentValueChange(triggerInfo: TriggerInfo) {
        const { oldValue, newValue, method } = triggerInfo
        if(isAtom(currentValues)) {
            updateIndicators(oldValue as T, false)
            updateIndicators(newValue as T, true)
        } else {
            // RxSet，有 add/delete/replace method
            let newItems: T[] = []
            let deletedItems: T[] = []
            if (method === 'add') {
                newItems = [triggerInfo.argv![0] as T]
            } else if (method === 'delete') {
                deletedItems = [triggerInfo.argv![0] as T]
            } else {
                [newItems, deletedItems] = triggerInfo.methodResult as [T[], T[]]
            }
            newItems.forEach((item) => {
                updateIndicators(item, true)
            })
            deletedItems.forEach((item) => {
                updateIndicators(item, false)
            })
        }
    }

    const stopAutoResetValue = autoResetValue ?
        new Computed(
            function(this: Computed){
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
            },
            function(_, triggerInfos: TriggerInfo[]) {
                // CAUTION 不能对非 splice 的 info 断言崩溃：sortSelf/reposition/swap
                //  触发 method='reorder'，set 触发 explicit key change，都是 source 的
                //  合法操作。reorder 不改变成员，无需回收；set 回收被替换掉的旧值
                //  （与 splice 删除项的语义一致）。
                triggerInfos.forEach((triggerInfo) => {
                    const { method, type, oldValue } = triggerInfo
                    if (method === 'splice') {
                        const deleteItems = triggerInfo.methodResult as T[] || []
                        // 批量删除时预构建存活集：把存活检查从 O(删除数 × 列表长)
                        // 摊平为 O(列表长 + 删除数)；单条删除保持零分配的 includes。
                        const survivors = deleteItems.length > 1 ? new Set(source.data) : undefined
                        deleteItems.forEach((item:T) => {
                            deleteCurrentValueIfItemRemoved(item, survivors)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        deleteCurrentValueIfItemRemoved(oldValue as T)
                    }
                    // reorder：成员不变，选中集不动
                })
            },
            true
        ) :
        undefined

    stopAutoResetValue?.run()

    // CAUTION 全量重建时必须清空记账(2026-H3 round3 GC 审计命中的记账无界类,
    //  同 depsMap host 摘除/pruneIndexKeyDeps 的规则:谁建条目谁给回收路径):
    //  computation 每次全量重算为每行 createNewIndicator,旧行的条目无人回收——
    //  force recompute/错误恢复路径下 Map 值升级 Set 后无界累积,且 currentValues
    //  每次变化都遍历写全部死 indicator(写放大)。行级增删的精确回收仍走
    //  deleteIndicator;这里只服务"全部行重建"的语义。
    function resetIndicators() {
        itemToIndicators.clear()
    }

    return {
        trackIndicators,
        trackCurrentValues,
        createNewIndicator,
        updateIndicatorsFromCurrentValueChange,
        stopAutoResetValue,
        deleteIndicator,
        resetIndicators,
        currentValues
    }
}




function createRxListWithSelectionInners<T>(source:RxList<T>, ...inners: SelectionInner[]) : RxList<[T, ...Atom<boolean>[]]>{

    function updateIndicatorsFromSourceChange(list: RxList<[T, ...Atom<boolean>[]]>, triggerInfo: TriggerInfo) {
        if (triggerInfo.method === 'splice') {
            const { argv } = triggerInfo
            const newItemsInArgs = argv!.slice(2)
            // CAUTION 先 splice 拿到被删除的行元组，再按 indicator 身份精确清理记账：
            //  旧实现按 methodResult 的 item 值清理，重复 item 下会把存活孪生行的
            //  记账一并误删（deleteIndicator 的重复值缺陷类）。
            const deletedRows = list.spliceArray(argv![0], argv![1], newItemsInArgs.map((item) => [item, ...inners.map(inner => inner.createNewIndicator(item))] as [T, ...Atom<boolean>[]]))
            deletedRows.forEach((row) => {
                // 稀疏行安全：越界 set（契约内透传）产生的洞位行是 undefined
                if (!row) return
                const [item, ...indicators] = row
                inners.forEach((inner, i) => inner.deleteIndicator(item, indicators[i]))
            })
        } else if (triggerInfo.method === 'reorder') {
            // CAUTION 行必须随源同步重排（indicator 挂在行元组上随行移动，成员与
            //  选中集都不变）。旧实现把 reorder 落进 explicit key change 分支，
            //  用 ITERATE_KEY（Symbol）当 index 去 set，选择列表从此与源失序。
            list.reorder(triggerInfo.argv![0] as Order[], triggerInfo.reorderInfo as ReorderPatchInfo | undefined)
        } else {
            //explicit key change
            const {  newValue, key } = triggerInfo
            // 非稠密 key:数组属性赋值,无行变化。物化会向行列表写属性并泄漏
            // itemToIndicators 记账条目(幽灵 EKC 等价类,忽略 ≡ 全量重算)。
            if (!isDenseIndexKey(key)) return
            const oldRow = list.set(key as number, [newValue as T, ...inners.map(inner => inner.createNewIndicator(newValue as T))] as [T, Atom<boolean>])
            // 被替换行的记账同步移除（旧实现从不清理：条目泄漏，重复 item 下
            //  还会让 currentValues 变化继续驱动已离场行的 indicator）
            if (oldRow) {
                const [oldItem, ...oldIndicators] = oldRow as [T, ...Atom<boolean>[]]
                inners.forEach((inner, i) => inner.deleteIndicator(oldItem, oldIndicators[i]))
            }
        }
    }

    return new RxList(
        function computation(this:Computed ) {
            inners.forEach(inner => {
                // 全量重建 = 所有旧行废弃:先清 indicator 记账再逐行重建(见 resetIndicators)
                inner.resetIndicators()
                inner.trackIndicators(this)
                inner.trackCurrentValues(this)
            })

            return source.data.map((item) => [item, ...inners.map(inner => inner.createNewIndicator(item))])
        },
        function applyPatch(this: RxList<[T, Atom<boolean>]>, _data, triggerInfos: TriggerInfo[]) {
            triggerInfos.forEach((triggerInfo) => {
                if (triggerInfo.source === source) {
                    // 来自 source 的变化，需要同步 indicators
                    updateIndicatorsFromSourceChange(this, triggerInfo)
                } else {
                    // 来自 currentValues 的变化，需要同步 indicators
                    inners.forEach(inner => {
                        if (triggerInfo.source === inner.currentValues) {
                            inner.updateIndicatorsFromCurrentValueChange(triggerInfo)
                        }
                    })
                }
            })
        },
        undefined,
        {
            onDestroy() {
                inners.forEach(inner => {
                    inner.stopAutoResetValue?.destroy()
                })
            }
        }
    )
}

type SelectionArgs<T> = [RxSet<T|number>|Atom<T|null|number>, boolean?]
export function createSelection<T>(source: RxList<T>, currentValues: SelectionArgs<T>[0], autoResetValue : SelectionArgs<T>[1] = false): RxList<[T, Atom<boolean>]> {
    return createRxListWithSelectionInners(source, createSelectionInner(source, currentValues, autoResetValue)) as  RxList<[T, Atom<boolean>]>
}

export function createSelections<T>(source: RxList<T>, ...args: SelectionArgs<T>[]): RxList<[T, ...Atom<boolean>[]]> {
    return createRxListWithSelectionInners(source, ...args.map(arg => createSelectionInner(source, ...arg)))
}

// 按 index 选中：currentValues 为 Atom 时单选，为 RxSet 时多选（选中的是位置，不随 item 移动）。
export function createIndexKeySelection<T>(source: RxList<T>, currentValues: RxSet<number>|Atom<null|number>, autoResetValue = false): RxList<[T, Atom<boolean>]> {

    function trackCurrentValues(list: Computed) {
        if (isAtom(currentValues)) {
            list.manualTrack(currentValues, TrackOpTypes.ATOM, 'value');
        } else {
            list.manualTrack(currentValues, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
        }
    }

    function trackIndicators(list: Computed) {
        list.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
        // set 只触发 EXPLICIT_KEY_CHANGE：不追踪的话行内容会与源永久失联
        list.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
    }

    function getSelectedIndexes(): Set<number> {
        if (isAtom(currentValues)) {
            const raw = currentValues.raw
            return raw !== null && raw !== undefined ? new Set([raw as number]) : new Set()
        }
        return new Set(currentValues.data)
    }

    function updateIndicatorsFromSourceChange(list: RxList<[T, Atom<boolean>]>, triggerInfo: TriggerInfo) {
        if (triggerInfo.method === 'splice') {
            const {  argv, methodResult } = triggerInfo
            const newItemsInArgs = argv!.slice(2)
            // CAUTION argv 是用户原始参数：start 归一化后才能参与 index 比较，
            //  deleteCount 用 methodResult（真实删除数）而不是 argv[1]（可能越界的用户输入）
            const insertCount = newItemsInArgs.length
            const deleteCount = (methodResult as unknown[] | undefined)?.length ?? 0
            // list 此刻尚未应用本次 splice，其长度即 splice 前的长度
            const startIndex = normalizeSpliceStart(argv![0], list.data.length)
            // CAUTION createNewIndicator 的参数语义是 index：新行的初始选中状态由
            //  落位的 index 决定，绝不能传 item——数值型 item 与某个选中 index 撞值时
            //  会被误置为选中。
            list.spliceArray(startIndex, deleteCount, newItemsInArgs.map((item, i) => [item, createNewIndicator(startIndex + i)] as [T, Atom<boolean>]))

            // index 选中语义：选中的是位置，不随 item 移动。增删不等长时
            // [startIndex + insertCount, ...) 的行整体平移，逐行按当前 index 重算
            // 指示器（选中集是稀疏的，两向 toggle 的旧算法在"选中 index 落在删除
            // 区间/多选相邻"等组合下会关错行）。等长替换时后续行不动，新行已按
            // index 初始化，无需修正。
            // CAUTION 洞位行安全（?.）：越界 set（契约内透传）会让行数组出洞，校正
            //  循环撞洞位直接 TypeError 且抛给 list.splice 调用方，违反"OOB set ×
            //  派生算子不崩溃且可恢复"等价类（sparseSetOperatorsSweep 只测了纯尾插,
            //  尾插不进本循环）。洞位行无 indicator 可校正，跳过与 map/filter 的
            //  行级 ?. 守卫一致。
            if (deleteCount !== insertCount) {
                const selectedIndexes = getSelectedIndexes()
                for (let i = startIndex + insertCount; i < list.data.length; i++) {
                    list.data[i]?.[1](selectedIndexes.has(i))
                }
            }
        } else if (triggerInfo.method === 'reorder') {
            // 行随源重排，但选中的 index 不动：重排后受影响区间逐行按新 index 校正
            // （洞位行 ?. 跳过，同上 splice 校正循环的说明）
            const reorderInfo = triggerInfo.reorderInfo as ReorderPatchInfo | undefined
            list.reorder(triggerInfo.argv![0] as Order[], reorderInfo)
            const selectedIndexes = getSelectedIndexes()
            const affected = reorderInfo?.affectedRange
            const start = affected ? Math.max(affected[0], 0) : 0
            const end = affected ? Math.min(affected[1] + 1, list.data.length) : list.data.length
            for (let i = start; i < end; i++) {
                list.data[i]?.[1](selectedIndexes.has(i))
            }
        } else {
            // explicit key change（set）：index 不变，行内容替换，选中状态由 index 决定
            const { newValue, key } = triggerInfo
            // 非稠密 key:数组属性赋值,无行变化(幽灵 EKC 等价类,忽略 ≡ 全量重算)
            if (!isDenseIndexKey(key)) return
            list.set(key as number, [newValue as T, createNewIndicator(key as number)])
        }
    }

    function updateIndicatorsFromCurrentValueChange(list: RxList<[T,  Atom<boolean>]>,triggerInfo: TriggerInfo) {
        const { oldValue, newValue, method } = triggerInfo

        if(isAtom(currentValues)) {
            list.data[oldValue as number]?.[1](false)
            list.data[newValue as number]?.[1](true)
        } else {
            // RxSet，有 add/delete/replace method
            let deleteItems: number[] = []
            let insertItems: number[] = []
            if (method === 'add') {
                insertItems = [triggerInfo.argv![0] as number]
            } else if (method === 'delete') {
                deleteItems = [triggerInfo.argv![0] as number]
            } else {
                // RxSet.replace 的协议是 [newItems, deletedItems]。
                [insertItems, deleteItems] = triggerInfo.methodResult as [number[], number[]]
            }


            (deleteItems as number[]).forEach((item) => {
                list.data[item]?.[1](false)
            })
            insertItems.forEach((item:number) => {
                list.data[item]?.[1](true)
            })
        }
    }

    function createNewIndicator(index: number) {
        return atom(isAtom(currentValues) ? currentValues.raw === index : currentValues.data.has(index))
    }

    const autoResetValueEffect = autoResetValue ?
        new Computed(
            function(this: Computed){
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
            },
            function(_, triggerInfos: TriggerInfo[]) {
                triggerInfos.forEach((triggerInfo) => {
                    const { method } = triggerInfo
                    // CAUTION 只有 splice 会改变长度；reorder（sortSelf/reposition/swap）
                    //  长度不变，无需回收越界 index，也绝不能对其断言崩溃。
                    if (method !== 'splice') return
                    const newLength = source.data.length
                    if (isAtom(currentValues)) {
                        if (currentValues.raw !== null && currentValues.raw >= newLength) {
                            currentValues(null)
                        }
                    } else {
                        // RxSet
                        currentValues.data.forEach((item) => {
                            if (item >= newLength) {
                                currentValues.delete(item)
                            }
                        })
                    }
                })
            },
            true
        ) :
        undefined

    autoResetValueEffect?.run([], true)

    return new RxList<[T, Atom<boolean>]>(
        function  computation(this: Computed) {
            trackCurrentValues(this)
            trackIndicators(this)

            return source.data.map((item, key) => [item, createNewIndicator(key)])
        },
        function applyPatch(this: RxList<[T, Atom<boolean>]>, _data, triggerInfos: TriggerInfo[]) {
            triggerInfos.forEach((triggerInfo) => {
                if (triggerInfo.source === source) {
                    // 来自 source 的变化，需要同步 indicators
                    updateIndicatorsFromSourceChange(this, triggerInfo)
                } else {
                    updateIndicatorsFromCurrentValueChange(this, triggerInfo)
                }
            })
        },
        undefined,
        {
            onDestroy() {
                autoResetValueEffect?.destroy()
            }
        }
    )

}

