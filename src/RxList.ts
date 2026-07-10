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
import {assert, spliceMany} from "./util.js";
import {ReactiveEffect} from "./reactiveEffect.js";
import {RxMap} from "./RxMap.js";
import {RxSet} from "./RxSet";

type MapOptions<U> = {
    beforePatch?: (triggerInfo: InputTriggerInfo) => any,
    scheduleRecompute?: DirtyCallback,
    ignoreIndex?: boolean,
    onCleanup?: (item: U) => any,
    skipItemEffect?: boolean
}

type MapCleanupFn = () => any

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
        for (const [index, dep] of indexDeps) {
            if (isDepEmpty(dep)) indexDeps.delete(index)
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
        const length = this.data.length
        if (length === 0) return []
        const hasIndexKeyDeps = this.pruneIndexKeyDeps()
        const hasAtomIndexes = !!this.atomIndexes
        if (length === 1 || hasIndexKeyDeps || hasAtomIndexes) return this.splice(0, length)

        this.pauseAutoTrack()
        const deletedItems = this.data.slice()
        this.data.length = 0
        this.trigger(this, TriggerOpTypes.METHOD, { method:'splice', key: ITERATE_KEY, argv: [0, length], methodResult: deletedItems })
        this.sendTriggerInfos()
        this.resetAutoTrack()
        return deletedItems
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
        this.pauseAutoTrack()

        const originLength = this.data.length
        const deleteItemsCount = Math.min(deleteCount, originLength - start)
        // 清扫空 index dep：曾被 at() 订阅、现已全部退订的列表要能回到 fast path
        const hasIndexKeyDeps = this.pruneIndexKeyDeps()
        const hasAtomIndexes = !!this.atomIndexes
        const canUseMetadataFastPath = !hasIndexKeyDeps && !hasAtomIndexes
        const isPureAppend = start === originLength && deleteCount === 0
        const isPureClear = start === 0 && deleteCount >= originLength && items.length === 0

        if (canUseMetadataFastPath && (isPureAppend || isPureClear)) {
            const result = spliceMany(this.data, start, deleteCount, items)
            this.trigger(this, TriggerOpTypes.METHOD, { method:'splice', key: ITERATE_KEY, argv: [start, deleteCount, ...items], methodResult: result })
            this.sendTriggerInfos()
            this.resetAutoTrack()
            return result
        }


        // CAUTION 不需要触发 length 的变化，因为获取  length 的时候得到就已经是个 computed 了。
        const newLength = originLength - deleteItemsCount + items.length
        const changedIndexEnd = deleteItemsCount !== items.length ? newLength : start + items.length
        // CAUTION 只对"实际有订阅者且落在受影响区间"的 index 记录 oldValue / 触发 SET：
        //  旧实现对 [start, changedIndexEnd) 逐 index 触发，一次中段 splice 是 O(移动范围)
        //  次 trigger；订阅通常是稀疏的（axii 每行订阅自己的 index），按订阅遍历后
        //  复杂度变为 O(订阅数)。触发保持升序，与旧实现的可观察顺序一致。
        let affected: [index: number, oldValue: T][] | undefined
        if (hasIndexKeyDeps) {
            for (const index of this._indexKeyDeps!.keys()) {
                if (index >= start && index < changedIndexEnd) {
                    (affected ?? (affected = [])).push([index, this.data[index]])
                }
            }
            if (affected && affected.length > 1) {
                affected.sort((a, b) => a[0] - b[0])
            }
        }
        const result = spliceMany(this.data, start, deleteCount, items)


        // CAUTION 无论有没有 indexKeyDeps 都要触发 Iterator_Key，
        //  特别这里注意，我们利用传了 key 就会把对应 key 的 dep 拿出来的特性来 trigger ITERATE_KEY.
        //  CAUTION 一定先 trigger method，这样可能后面某些被删除的 atomIndexes 变化就不需要了。
        this.trigger(this, TriggerOpTypes.METHOD, { method:'splice', key: ITERATE_KEY, argv: [start, deleteCount, ...items], methodResult: result })
        if (affected) {
            for (const [index, oldValue] of affected) {
                this.trigger(this, TriggerOpTypes.SET, { key: index, newValue: this.data[index], oldValue })
            }
        }

        // CATION 特别注意这里 atomIndexes 的变化也要先 catch 住
        notifier.createEffectSession()
        this.sendTriggerInfos()

        if (this.atomIndexes) {
            spliceMany(this.atomIndexes, start, deleteCount, items.map((_, index) => atom(index + start)))
            for (let i = start; i <changedIndexEnd; i++) {
                // 注意这里的 ?. ，因为 splice 之后可能长度不够了。
                this.atomIndexes[i]?.(i)
            }
        }
        notifier.digestEffectSession()

        this.resetAutoTrack()
        return result
    }
    // 显式 set 某一个 index 的值
    // CAUTION set 的契约是"替换已存在的稠密行"。越界/负数/非整数 key 属于 out-of-contract 用法：
    //  行为与普通数组赋值一致（可能产生稀疏数组、length computed 不会更新），trigger 原样透传
    //  key，由下游（axii/axle 等渲染框架）自行拒绝或归一化。不要在这里改走 splice 语义：
    //  下游的结构化错误契约（以及 set(Infinity) 这类 key）都依赖透传行为。
    set(index: number, value: T) {
        const oldValue = this.data[index]
        this.data[index] = value

        // 这里还是用 trigger TriggerOpTypes.SET，因为系统在处理 TriggerOpTypes.SET 的时候还会对 listLike 的数据 触发 ITERATE_KEY。
        this.trigger(this, TriggerOpTypes.SET, { key: index, newValue: value, oldValue})
        this.trigger(this, TriggerOpTypes.EXPLICIT_KEY_CHANGE, { key: index, newValue: value, oldValue, methodResult: oldValue})
        this.sendTriggerInfos()

        return oldValue
    }
    reorder(newOrder: Order[], reorderInfo = createReorderPatchInfo('reorder', newOrder)) {
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
                oldIndexAtoms[i]?.(newIndex)
                this.atomIndexes![newIndex] = oldIndexAtoms[i]!
            }
        })

        this.trigger(this, TriggerOpTypes.METHOD, { method:'reorder', key: ITERATE_KEY, argv: [newOrder], reorderInfo })
        this.sendTriggerInfos()
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
    // 在按 compare 有序的数组中定位与 item 引用相等的元素：
    // 二分到相等区间后线性扫描（同序元素可能有多个）。找不到（比如元素的排序键
    // 在删除前被外部改写、数组已不完全有序）返回 -1，调用方回退 indexOf 兜底。
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
            if (arr[i] === item) return i
        }
        return -1
    }
    private static locateInSorted<S>(arr: S[], item: S, compare: (a: S, b: S) => number): number {
        const found = RxList.binarySearchFind(arr, item, compare)
        return found !== -1 ? found : arr.indexOf(item)
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
                // Incremental updates
                triggerInfos.forEach((info) => {
                    const { method, argv, oldValue, newValue, methodResult } = info
                    // method could be 'splice' (array insertion/removal) or an explicit key change
                    if (method === 'splice') {
                        // 1) remove items（有序数组用二分定位，indexOf 仅兜底）
                        const deletedItems = (methodResult as T[]) || []
                        deletedItems.forEach((item) => {
                            const idx = RxList.locateInSorted(this.data, item, compare!)
                            if (idx !== -1) {
                                this.splice(idx, 1)
                            }
                        })
                        // 2) insert new items in sorted order
                        const newItems = argv!.slice(2) as T[]
                        newItems.forEach((item) => {
                            const insertIndex = RxList.binarySearchInsert(this.data, item, compare!)
                            this.splice(insertIndex, 0, item)
                        })
                    } else {
                        // explicit key change: remove old, insert new
                        if (oldValue !== undefined) {
                            const idx = RxList.locateInSorted(this.data, oldValue as T, compare!)
                            if (idx !== -1) {
                                this.splice(idx, 1)
                            }
                        }
                        if (newValue !== undefined) {
                            const insertIndex = RxList.binarySearchInsert(this.data, newValue as T, compare!)
                            this.splice(insertIndex, 0, newValue as T)
                        }
                    }
                })
            }
        )
    }
    // CAUTION 这里手动 track index dep 的变化，是为了在 splice 的时候能手动去根据订阅的 index dep 触发，而不是直接触发所有的 index key。
    at(index: number): T|undefined{
        const dep = notifier.track(this, TrackOpTypes.GET, index)
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

        // CAUTION cleanupFns 是用户自己用 context.onCleanup 收集的，因为可能用到 mapFn 中的局部变量
        //  如果可以直接从 mapFn return value 中来销毁副作用，那么应该使用 options.onCleanup 来注册一个统一的销毁函数，这样能提升性能，不需要建立 cleanupFns 数组。
        let cleanupFns: MapCleanupFn[]|undefined
        // 行级依赖探测 effect：整个 map 产物复用一个实例（说明见类定义处）。
        // detached 创建：不能被 map computation 收集为 child。
        let itemProbe: MapItemDependencyProbe | undefined

        // 探测一行：mapFn 恰好执行一次；捕获到依赖/子 effect 时升级为常驻的行级
        // Computed（探测期间建立的订阅原样转移），无依赖行零对象分配。
        // 返回 [mapFn 返回值, 行级 effect frame]。
        function runItemAndCollectEffect(list: RxList<U>, item: T, index: number, mapContext: MapContext | undefined): [U, ReactiveEffect[]] {
            const probe = itemProbe ?? (itemProbe = ReactiveEffect.createDetached(() => new MapItemDependencyProbe()))
            const value = probe.probe(() => mapFn(item, source.atomIndexes?.[index]!, mapContext!)) as U
            if (!probe.hasCaptures()) return [value, EMPTY_ITEM_FRAME]

            // CAUTION 只有依赖变化需要重算的行才需要 index atom（重算时行的位置可能已变化）；
            //  仅含子 effect 的行（作为 frame 容器）不会被 trigger，getter 不会执行。
            let newItemIndex: Atom<number>|undefined
            if (probe.deps.length > 0) {
                if (!addedAtomIndexesDep) {
                    source.addAtomIndexesDep()
                    addedAtomIndexesDep = true
                }
                newItemIndex = source.atomIndexes![index]!
            }
            const rowComputed = new Computed(() => {
                // CAUTION 特别注意这里面的变量，我们只希望 track 用户 mapFn 里面用到的外部 reactive 对象，不希望 track 到自己的 key/index。
                if (newItemIndex) {
                    list.set(newItemIndex.raw, mapFn(source.data[newItemIndex.raw], newItemIndex, mapContext!))
                }
            }, undefined, true)
            probe.transferCapturesTo(rowComputed)
            // 探测已经完成了首次计算，行级 Computed 从 CLEAN 开始（下次依赖触发时正常重算）
            rowComputed._status = STATUS_CLEAN
            return [value, [rowComputed]]
        }

        return new RxList(
            function computation(this: RxList<U>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                cleanupFns = useContext ? [] : undefined

                const result: U[] = []
                const sourceData = source.data
                for (let i = 0; i < sourceData.length; i++) {
                    const mapContext: MapContext|undefined = useContext ? {
                        onCleanup(fn: MapCleanupFn) {
                            cleanupFns![i] = fn
                        }
                    } : undefined

                    if (options?.skipItemEffect) {
                        result[i] = mapFn(sourceData[i], source.atomIndexes?.[i]!, mapContext!)
                        this.effectFramesArray![i] = []
                    } else {
                        const [value, frame] = runItemAndCollectEffect(this, sourceData[i], i, mapContext)
                        result[i] = value
                        this.effectFramesArray![i] = frame
                    }
                }

                return result
            },
            function applyMapArrayPatch(this: RxList<U>, _data, triggerInfos) {
                triggerInfos.forEach((triggerInfo) => {

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
                        const effectFrames: ReactiveEffect[][] = []
                        const newCleanups: MapCleanupFn[] = []
                        const newItems = newItemsInArgs.map((newItemsInArg, index) => {
                            const mapContext: MapContext|undefined = useContext ? {
                                onCleanup(fn: MapCleanupFn) {
                                    newCleanups![index] = fn
                                }
                            } : undefined
                            let newItem: U
                            const newIndex = index + argv![0]!
                            if (options?.skipItemEffect) {
                                newItem = mapFn(newItemsInArg, source.atomIndexes?.[newIndex]!, mapContext!)
                                effectFrames![index] = []
                            } else {
                                const [value, frame] = runItemAndCollectEffect(this, newItemsInArg, newIndex, mapContext)
                                newItem = value
                                effectFrames![index] = frame
                            }
                            return newItem!
                        })
                        const deletedItems = this.spliceArray(argv![0], argv![1], newItems)
                        const deletedFrames = spliceMany(this.effectFramesArray!, argv![0], argv![1], effectFrames)
                        deletedFrames.forEach((frame) => {
                            frame.forEach((effect) => {
                                this.destroyEffect(effect)
                            })
                        })
                        // 更新和执行 cleanupFns
                        if (useContext && cleanupFns?.length) {
                            // CAUTION 这里要把删除的 effect 的 cleanup 都执行一遍
                            //  如果能从 return value 中进行销毁，应该使用 options.onCleanup 来注册一个统一的销毁函数，这样能提升性能。
                            const deletedCleanupFns = spliceMany(cleanupFns, argv![0], argv![1], newCleanups)
                            deletedCleanupFns.forEach((fn) => {
                                fn?.()
                            })
                        }
                        // 统一的销毁函数
                        if(options?.onCleanup) {
                            deletedItems.forEach((item) => {
                                options.onCleanup!(item)
                            })
                        }
                    } else if(method === 'reorder') {
                        // 排序会触发所有 map 出来的元素同样计算
                        this.reorder(argv![0]! as Order[], triggerInfo.reorderInfo as ReorderPatchInfo | undefined)
                    } else {
                        // explicit key change
                        // CAUTION add/update 一定都要全部重新从 source 里面取，因为这样才能得到正确的 proxy。newValue 是 raw data，和 mapFn 里面预期拿到的不一致。
                        // 没有 method 说明是 explicit_key_change 变化
                        const index = key as number
                        const getFrame = this.collectEffect()
                        const mapContext: MapContext|undefined = useContext ? {
                            onCleanup(fn: MapCleanupFn) {
                                cleanupFns![index] = fn
                            }
                        } : undefined
                        const oldItem = this.data.at(index)!
                        const oldCleanupFn = cleanupFns?.[index]

                        this.set(index, mapFn(source.at(index)!, source.atomIndexes?.[index]!, mapContext!))
                        const newFrame = getFrame() as ReactiveEffect[]
                        this.effectFramesArray![index]?.forEach((effect) => {
                            this.destroyEffect(effect)
                        })
                        this.effectFramesArray![index] = newFrame

                        if (oldCleanupFn) {
                            oldCleanupFn()
                        }
                        if(options?.onCleanup) {
                            options.onCleanup(oldItem)
                        }

                    }
                })
            },
            options?.scheduleRecompute,
            {
                onDestroy(this: RxList<U>)  {
                    itemProbe?.destroy()
                    itemProbe = undefined
                    if (addedAtomIndexesDep) {
                        source.removeAtomIndexesDep()
                    }
                    if (cleanupFns) {
                        cleanupFns.forEach((fn) => {
                            fn()
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
                    reduceFn(placeholder, source.data[i], i)
                    this.effectFramesArray![i] = getFrame() as ReactiveEffect[]
                }

                const result = placeholder.data
                placeholder.destroy()
                delete placeholder.data
                return result
            },
            function applyMapArrayPatch(this: U, _data:any, triggerInfos: TriggerInfo[]) {
                // 只有纯粹的新增在末尾新增，是可以使用增量计算的
                const shouldRecompute = triggerInfos.some((triggerInfo) => {
                    const { method , argv   } = triggerInfo
                    return !(method === 'splice' && argv![0] === source.data.length - argv!.slice(2).length && argv![1] === 0)
                })

                if(shouldRecompute) return false

                triggerInfos.forEach((triggerInfo) => {
                    const { argv   } = triggerInfo
                    const originLength = source.data.length
                    // CAUTION 这里重新从已经改变的  source 去读，才能重新被 reactive proxy 处理，和全量计算时收到的参数一样
                    const newItemsInArgs = argv!.slice(2)
                    for(let i = 0; i < newItemsInArgs.length; i++) {
                        const getFrame = ReactiveEffect.collectEffect!()
                        reduceFn(this, newItemsInArgs[i], i + originLength)
                        this.effectFramesArray![i] = getFrame() as ReactiveEffect[]
                    }
                })
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
                // 只有纯粹的新增在末尾新增，是可以使用增量计算的
                const shouldRecompute = triggerInfos.some((triggerInfo) => {
                    const { method , argv   } = triggerInfo
                    return !(method === 'splice' && argv![0] === source.data.length - argv!.slice(2).length && argv![1] === 0)
                })

                if(shouldRecompute) return false

                triggerInfos.forEach((triggerInfo) => {
                    const { argv } = triggerInfo
                    const originLength = source.data.length
                    // CAUTION 这里重新从已经改变的  source 去读，才能重新被 reactive proxy 处理，和全量计算时收到的参数一样
                    const newItemsInArgs = argv!.slice(2)
                    for(let i = 0; i < newItemsInArgs.length; i++) {
                        data(reduceFn(data.raw, newItemsInArgs[i], i + originLength))
                    }
                })
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
        const searchedItemAndIndexes: { item:T, index:number, deleted:boolean }[] = []

        let trackTargetToSearchItem: WeakMap<any, Set<{ item:T, index:number, deleted:boolean }>> = new WeakMap()

        const disposeAll = () => {
            searchedItemAndIndexes.length = 0
            trackTargetToSearchItem = new WeakMap()
        }

        function searchAndRemember(start:number, end: number, resultComputed: Computed) {
            for(let current=start; current < Math.min(end, source.data.length);current++) {
                const matchResult = matchAndRemember(current, resultComputed)
                if (matchResult) {
                    // 删掉后面的
                    // FIXME 似乎没有处理 trackTargetToSearchItem 中的 cache
                    const deletedItems = searchedItemAndIndexes.splice(current+1)
                    deletedItems.forEach(item => item.deleted = true)
                    return current
                }

            }
            return -1
        }

        function matchAndRemember(current:number, resultComputed: Computed) {
            const currentItem =  {
                item: source.data[current],
                index:current,
                deleted:false
            }
            searchedItemAndIndexes[current] =currentItem
            resultComputed.autoTrack()
            const getFrame = notifier.collectTrackTarget()
            const matchResult = matchFn(source.data[current])
            const trackTargets = getFrame()
            resultComputed.resetAutoTrack()

            trackTargets.forEach((target) => {
                let items = trackTargetToSearchItem.get(target)
                if (!items) {
                    trackTargetToSearchItem.set(target, items = new Set())
                }
                items.add(currentItem)
            })
            return matchResult
        }

        function checkOne(index: number) {
            if (matchFn(source.data[index])) {
                result(index)
                searchedItemAndIndexes.splice(index+1)
            }
        }

        const result = computed<number>(
            function computation(this: Computed) {
                disposeAll()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
                return searchAndRemember(0, Infinity, this)
            },
            function applyPatch(this: Computed, data: Atom<number>, triggerInfos){
                let patchSuccess = undefined
                // 每次 patch 都需要重新注册所有依赖。
                this.cleanup()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)

                // CAUTION 用 for...of 而不是 every：every 的回调若不显式 return true 会提前终止，
                //  导致一次 batch 里的多条 triggerInfo 只处理第一条。
                for (const triggerInfo of triggerInfos) {
                    const { method , argv  ,key, source: triggerSource } = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    let startFindingIndex = Infinity
                    if (triggerSource === source ) {
                        if (method === 'splice') {
                            const startIndex = argv![0] as number
                            // 可能新增了更小的能找到的，都从 startIndex 开始重新算。
                            if (this.data.raw == -1 || startIndex <= this.data.raw) {
                                startFindingIndex = startIndex
                            }

                        } else {
                            // explicit key change
                            if (this.data.raw === key) {
                                // 刚好把找到的弄没了
                                startFindingIndex = key as number
                            } else if(this.data.raw === -1 || (key as number) < this.data.raw) {
                                // 当前没有匹配（-1）时任何位置的变化都可能产生新匹配；
                                // 有匹配时只有更小的 index 才可能替换。快速验证这一个是不是新的 match。
                                checkOne(key as number)
                            }
                        }

                        // 需要从 startFindingIndex 开始重找，startFindingIndex 前面不需要
                        if (startFindingIndex !== Infinity) {
                            data(searchAndRemember(startFindingIndex, Infinity, this))
                        }
                    } else {
                        // 任何其他变化都完全重算
                        // 元素计算的内部变化。找到受影响的 items，从小的开始计算。一旦找到就停下。
                        //  一直到所有响应完还有没有找到的话，就继续 search。
                        // CAUTION 一定要切片，否则后面 matchAndRemember 会死循环
                        const itemCandidateSet = trackTargetToSearchItem.get(triggerSource)
                        if (itemCandidateSet) {
                            const itemCandidates = Array.from(itemCandidateSet)

                            let newIndex = -1
                            let lastMatchedChanged = false
                            for(const item of itemCandidates) {
                                if (!item.deleted) {
                                    // 重算的时候就要把上次的删掉，因为 matchAndRemember 中会重新生成个新对象。
                                    itemCandidateSet!.delete(item)
                                    const matchResult = matchAndRemember(item.index, this)
                                    if (!lastMatchedChanged && item.index ===data.raw) lastMatchedChanged = true
                                    if (matchResult) {
                                        // 删掉后面的
                                        // FIXME 更好地处理 trackTargetToSearchItem 中的 cache
                                        const deletedItems = searchedItemAndIndexes.splice(item.index+1)
                                        deletedItems.forEach(item => item.deleted = true)
                                        newIndex = item.index
                                        break
                                    }
                                } else {
                                    // FIXME 顺便删除一下，应该有更好的方式
                                    trackTargetToSearchItem.get(triggerSource)!.delete(item)
                                }
                            }
                            // 只要找到了，index 肯定更小，应为我们是往前面建立的观察
                            if (newIndex!==-1) {
                                data(newIndex)
                            } else {
                                // TODO 上一次的值如果也受影响了变成不匹配的了，并且受影的也没有匹配的，就要从上一次继续往后搜索
                                if (lastMatchedChanged) {
                                    data(searchAndRemember(data.raw+1, Infinity, this))
                                }
                            }
                        } else {
                            // 未知来源的变化，无法增量处理，提前结束并触发全量重算
                            patchSuccess = false
                            break
                        }
                    }
                }
                // 显式 return false 触发重算
                return patchSuccess
            },
            true,
            {
                onDestroy:disposeAll
            }
        )

        return result!
    }

    filter(filterFn: (item:T) => boolean): RxList<T> {
        const filtered = new RxList<T>([])
        const mapList = this.map((item, _, {onCleanup}) => {
            const remove = () => {
                const index =  filtered.data.indexOf(item)
                if (index !== -1) {
                    filtered.splice(index, 1)
                }
            }

            return computed(({lastValue} ) => {
                const matched = filterFn(item)
                if (matched) {
                    if (!lastValue.raw) {
                        if (item === this.data[0]) {
                            filtered.unshift(item)
                        } else {
                            filtered.push(item)
                        }
                    }
                } else {
                    // 第一次没匹配上不需要执行 remove，节省一下性能。
                    if (lastValue.raw === true) remove()
                }
                return matched
            }, undefined, true, {
                onDestroy() {
                    remove()
                }
            })
        }, { ignoreIndex: true})

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
                triggerInfos.forEach((triggerInfo) => {
                    const { method , argv  ,key, oldValue, newValue, methodResult, type} = triggerInfo
                    assert(method === 'splice' || key !== undefined, 'trigger info has no method and key')

                    if (method === 'splice') {
                        const deleteItems = methodResult as T[] || []
                        deleteItems.forEach((item) => {
                            const groupKey = getKey(item)
                            if (this.data.has(groupKey)) {
                                this.data.get(groupKey)!.splice(this.data.get(groupKey)!.data.indexOf(item), 1)
                            }
                        })

                        // 如果是从头插入，要逆序遍历 unshift 才能保持正确顺序
                        const newItemsInArgs = argv!.slice(2)
                        if (argv![0] === 0) {
                            newItemsInArgs.reverse()
                        }
                        const insertAtHead = argv![0] === 0

                        // 先分好组，再一次性操作，可以合并 info，还能间接提高 dom 操作性能。
                        const newGroupedItems = new Map<any, T[]>()
                        newItemsInArgs.forEach((item) => {
                            const groupKey = getKey(item)
                            if (!newGroupedItems.has(groupKey)) {
                                newGroupedItems.set(groupKey,[])
                            }
                            // CAUTION 这里并不能真正保证 group 里面的顺序和原来的一致。只能尽量处理首位情况。
                            if (argv![0] === 0) {
                                newGroupedItems.get(groupKey)!.unshift(item)
                            } else {
                                newGroupedItems.get(groupKey)!.push(item)
                            }
                        })

                        newGroupedItems.forEach((group, key) => {
                            if (!this.data.has(key)) {
                                this.set(key, new RxList(group))
                            } else {
                                const groupList = this.data.get(key)!
                                if (insertAtHead) {
                                    groupList.spliceArray(0, 0, group)
                                } else {
                                    groupList.spliceArray(groupList.data.length, 0, group)
                                }
                            }
                        })

                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // explicit key change
                        // CAUTION 用 undefined 判断而不是 truthy，oldValue 可能是 0/''/false 等合法值
                        if (oldValue !== undefined) {
                            const oldGroupKey = getKey(oldValue as T)
                            this.data.get(oldGroupKey)!.splice(this.data.get(oldGroupKey)!.data.indexOf(oldValue as T), 1)
                        }

                        const newGroupKey = getKey(newValue as T)
                        if (!this.data.has(newGroupKey)) {
                            this.set(newGroupKey, new RxList([]))
                        }
                        this.data.get(newGroupKey)!.push(newValue as T)
                    }
                })
            }
        )
    }

    indexBy(inputIndexKey: keyof T|((item: T) => any)) {
        const source = this
        return new RxMap<any, T>(
            function computation(this: RxMap<any, T>) {
                const map = new Map()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                for (let i = 0; i < source.data.length; i++) {
                    const item = source.data[i]
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
                            const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(item) : item[inputIndexKey]
                            this.delete(indexKey)
                        })
                        const newItemsInArgs = argv!.slice(2)
                        newItemsInArgs.forEach((item) => {
                            const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(item) : item[inputIndexKey]

                            assert(!this.data.has(indexKey), 'indexBy key is already exist')
                            this.set(indexKey, item)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // explicit key change
                        const indexKey = typeof inputIndexKey === 'function' ? inputIndexKey(oldValue as T) : (oldValue as T)[inputIndexKey]
                        this.delete(indexKey)
                        const newKey = typeof inputIndexKey === 'function' ? inputIndexKey(newValue as T) : (newValue as T)[inputIndexKey]
                        this.set(newKey, newValue as T)
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
        return new RxMap<T extends [any, any] ? T[0] : any, T extends [any, any] ? T[1] : any>(
            function computation(this: RxMap<any, T>) {
                const map = new Map()
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE);
                for (let i = 0; i < source.data.length; i++) {
                    const [key, value] = source.data[i] as [any, any]
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
                        const deleteItems = methodResult as [any, any][] || []
                        deleteItems.forEach(([indexKey]) => {
                            this.delete(indexKey)
                        })
                        const newItemsInArgs = argv!.slice(2) as [any, any][]
                        newItemsInArgs.forEach(([indexKey, value]) => {
                            this.set(indexKey, value)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // explicit key change
                        const indexKey = (oldValue as [any, any])[0]
                        this.delete(indexKey)
                        const [newKey, newItem] = newValue as [any, any]
                        this.set(newKey, newItem)
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
                            this.delete(item)
                        })
                        const newItemsInArgs = argv!.slice(2)
                        newItemsInArgs.forEach((item) => {
                            this.add(item)
                        })
                    } else if (type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
                        // explicit key change
                        this.delete(oldValue as T)
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

    // FIXME onUntrack 的时候要把 indexKeyDeps 里面的 dep 都删掉。因为 Effect 没管这种情况。
    /**
     * @internal
     */
    onUntrack(_effect: ReactiveEffect) {

    }
    destroy() {
        // CAUTION 用 _length 判断：length 是惰性 getter，直接访问会先创建再销毁
        if (this._length) destroyComputed(this._length)
        super.destroy()
        this.effectFramesArray?.forEach((frames) => {
          frames.forEach((frame) => {
            this.destroyEffect(frame)
          })
        })
        this._indexKeyDeps?.clear()
        this.atomIndexes = undefined
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
                // Figure out which source changed, then incrementally update
                triggerInfos.forEach(info => {
                    const sourceIndex = sources.indexOf(info.source as RxList<T>)
                    if (sourceIndex === -1) {
                        // Some unexpected source
                        return
                    }

                    // Calculate offset of that source in the final array
                    let offset = 0
                    for (let i = 0; i < sourceIndex; i++) {
                        offset += sources[i].data.length
                    }

                    const { method, argv, oldValue, newValue, methodResult } = info
                    if (method === 'splice') {
                        // old items to remove
                        const deletedItems = (methodResult as T[]) || []
                        deletedItems.forEach(d => {
                            const idx = this.data.indexOf(d)
                            if (idx !== -1) {
                                this.splice(idx, 1)
                            }
                        })

                        // new items to insert
                        const newItems = argv!.slice(2) as T[]
                        // insertion index = offset + argv![0]
                        let insertPos = offset + (argv![0] as number)
                        // clamp insertPos if user spliced out-of-bounds
                        insertPos = Math.min(Math.max(insertPos, 0), this.data.length)
                        this.spliceArray(insertPos, 0, newItems)
                    } else {
                        // explicit key change
                        if (oldValue !== undefined) {
                            const idx = this.data.indexOf(oldValue as T)
                            if (idx !== -1) {
                                this.splice(idx, 1)
                            }
                        }
                        if (newValue !== undefined) {
                            // For an in-place "set" or other change, we insert at correct offset
                            // That offset + (some approximate index). The simplest is to do a push,
                            // or correct offset if available. We'll pick an offset that places it
                            // near original. Without the old index, we approximate by pushing:
                            // (For a better approach, you'd track the original item's index.)
                            this.splice(offset + sources[sourceIndex].data.indexOf(newValue as T), 0, newValue as T)
                        }
                    }
                })
            }
        )
    }

    public slice(start?: number, end?: number): RxList<T> {
        const source = this
        // handle negative or undefined arguments
        start = start ?? 0
        end = end ?? Infinity
        
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
                for(const info of triggerInfos) {
                    // ensure it's from this source
                    if (info.source !== source) return false
                    // reorder（sortSelf/reposition/swap）会改变区间内元素的相对顺序，
                    // 无法用区间差量表达，直接全量重算。
                    if (info.method === 'reorder') return false
                    const idxs = clampIndexes(source.data.length)
                    // 现在不合法了，清空
                    if (!idxs || !lastIndexes) {
                        return false
                    }

                    // 原来和现在都合法
                    const { method, argv, newValue, methodResult: deletedItems, key } = info

                    if (method === 'splice') {
                        const insertedItems = argv!.slice(2) as T[]
                        if (deletedItems.length === 0 && insertedItems.length === 0) return

                        const startArgv = argv![0]  as number
                        const lastSourceLength = source.data.length - insertedItems.length + deletedItems.length
                        // 如果 start 参数为负数，按 splice 语义从末尾往前修正。
                        const spliceStart = startArgv! < 0 ? Math.max(0, lastSourceLength + startArgv!) : Math.min(startArgv!, lastSourceLength)
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
                                this.splice(ucHead[1]-ucHead[0], ucTailOldIndex! - (ucHead[1]-ucHead[0]), ...source.data.slice(ucHead[1], ucTail[0]))
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
                            this.spliceArray(0, 0, source.data.slice(idxs[0], oldStart))
                        } else if(oldStart! < idxs[0]) {
                            this.splice(0, idxs[0]-oldStart!)
                        }

                        if (oldEnd! < idxs[1]) {
                            this.spliceArray(this.data.length, 0, source.data.slice(oldEnd, idxs[1]))
                        } else if(oldEnd! > idxs[1]) {
                            this.splice(idxs[1] - idxs[0], Infinity)
                        }

                        lastIndexes = idxs
                    } else {
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

    const itemToIndicator: Map<any, Atom<boolean>> = new Map()

    function createNewIndicator(item:T) {
        const indicator = atom(isAtom(currentValues) ? currentValues.raw === item : currentValues.data.has(item))
        itemToIndicator.set(item, indicator)
        return indicator
    }

    function deleteCurrentValueIfItemRemoved(item:T) {
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

    function deleteIndicator(item:T) {
        itemToIndicator.delete(item)
    }


    function updateIndicatorsFromCurrentValueChange(triggerInfo: TriggerInfo) {
        const { oldValue, newValue, method } = triggerInfo
        if(isAtom(currentValues)) {
            itemToIndicator.get(oldValue as T)?.(false)
            itemToIndicator.get(newValue as T)?.(true)
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
                const indicator = itemToIndicator?.get(item)
                indicator?.(true)
            })
            deletedItems.forEach((item) => {
                const indicator = itemToIndicator?.get(item)
                indicator?.(false)
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
                triggerInfos.forEach((triggerInfo) => {
                    const { method } = triggerInfo
                    assert(method === 'splice', 'currentValues can only support splice')
                    const deleteItems = triggerInfo.methodResult
                    deleteItems.forEach((item:T) => {
                        deleteCurrentValueIfItemRemoved(item)
                    })
                })
            },
            true
        ) :
        undefined

    stopAutoResetValue?.run()

    return {
        trackIndicators,
        trackCurrentValues,
        createNewIndicator,
        updateIndicatorsFromCurrentValueChange,
        stopAutoResetValue,
        deleteIndicator,
        currentValues
    }
}




function createRxListWithSelectionInners<T>(source:RxList<T>, ...inners: SelectionInner[]) : RxList<[T, ...Atom<boolean>[]]>{

    function updateIndicatorsFromSourceChange(list: RxList<[T, ...Atom<boolean>[]]>, triggerInfo: TriggerInfo) {
        if (triggerInfo.method === 'splice') {
            const { methodResult , argv } = triggerInfo
            const newItemsInArgs = argv!.slice(2)
            const deleteItems: T[] = methodResult || []
            deleteItems.forEach((item) => {
                inners.forEach(inner => inner.deleteIndicator(item))
            })
            list.spliceArray(argv![0], argv![1], newItemsInArgs.map((item) => [item, ...inners.map(inner => inner.createNewIndicator(item))] as [T, ...Atom<boolean>[]]))
        } else {
            //explicit key change
            const {  newValue, key } = triggerInfo
            list.set(key as number, [newValue as T, ...inners.map(inner => inner.createNewIndicator(newValue as T))] as [T, Atom<boolean>])
        }
    }

    return new RxList(
        function computation(this:Computed ) {
            inners.forEach(inner => {
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

// TODO multiple
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
    }


    function updateIndicatorsFromSourceChange(list: RxList<[T, Atom<boolean>]>, triggerInfo: TriggerInfo) {
        if (triggerInfo.method === 'splice') {
            const {  argv } = triggerInfo
            const newItemsInArgs = argv!.slice(2)
            list.spliceArray(argv![0], argv![1], newItemsInArgs.map((item) => [item, createNewIndicator(item)] as [T, Atom<boolean>]))

            const deleteCount = argv![1]
            const insertCount = newItemsInArgs.length

            if (deleteCount !== insertCount) {
                const startIndex = argv![0] as number

                const selectedValues = isAtom(currentValues) ? (currentValues.raw !== null ? [currentValues.raw] : []) : [...currentValues.data]
                // 因为 index 产生了变化，所以要更新 indicator
                selectedValues.forEach((value) => {
                    const index = value as number
                    if (index < list.data.length ) {
                        // 只有 index 在后面的才是还存在，并且受了影响需要处理的。
                        if (index >= startIndex && deleteCount !== insertCount) {
                            const indexAfterChange = index + insertCount - deleteCount
                            const oldIndexIndicator = list.data.at(indexAfterChange)![1]
                            oldIndexIndicator(false)
                        }
                        const newIndicator = list.data.at(index)![1]
                        newIndicator?.(true)
                    }
                })
            }
        }
        // 不需要处理 explicit key change
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
                [deleteItems, insertItems] = triggerInfo.methodResult as [number[], number[]]
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
                    assert(method === 'splice', 'currentValues can only support splice')
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

