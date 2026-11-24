import {
    ApplyPatchType,
    CallbacksType,
    computed,
    Computed,
    destroyComputed,
    DirtyCallback,
    getComputedInternal,
    GetterType,
    SkipIndicator
} from "./computed.js";
import {ITERATE_KEY, notifier} from "./notify.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {Atom} from "./atom.js";
import {RxList} from "./RxList.js";
import {ReactiveEffect, reportTeardownError} from "./reactiveEffect.js";
import {assert, isMap, warn} from "./util.js";

type EntryType = [any, any][]
type PlainObjectType = {
    [key: string]: any
}
/**
 * argv 里的 key → 容器里实际存的 key。原生 Map 把 -0 存成 +0，而载荷按协议透传调用方的
 * 原始参数（AGENTS「argv 透传，内部消费者独立归一化」）；按 argv 重放出 key 序列的内部
 * 消费者必须经它，否则增量结果持有 -0、全量重算（[...map.keys()]）持有 +0，在 Object.is
 * 下分叉（README §3「batch 退出后 ≡ 终态全量重算」、§4「元素身份按 Object.is」）。
 */
function normalizeKey<K>(key: K): K {
    return (key as unknown) === 0 ? 0 as unknown as K : key
}
/**
 * values()/entries() 的共享构造：直接消费源 Map 的 METHOD info 的 patch 列表。
 * （模块级函数而不是原型方法：内部设施，不进 RxMap 的公开成员面。）
 *
 * CAUTION 不能再写成 keys().map(key => this.get(key))（旧实现）：行级 effect 订阅的是
 *  key dep，删除 key 时它在 keys 列表 patch **之前**被唤醒，此刻 Map 已删、keys 未删，
 *  get(key) 读出 undefined 并写进派生列表——下游先收到一条 set(index, undefined)
 *  （Map 从未持有过的值），随后才是 splice。values().map(v => v.name) 这类下游当场
 *  TypeError，抛给 map.delete()/clear()/replace() 的调用方（经 indexBy/groupBy/toMap
 *  得到的派生 Map 同理）。调换派发顺序（METHOD 先于 key 级触发）只挡得住部分时序：
 *  行已因更早的 set 入队、订阅者在 digest 中重入 delete 时，行仍先于 keys 的第二轮
 *  patch 运行。这里改为只按 METHOD info 的**因果序**重放——值取自 info.argv（操作时的
 *  值），位置由本地 key 序逐条维护（操作时位置），不再回读 Map 的终态，中间值从构造上
 *  不存在；同时省掉每个 key 一个行级 Computed + index atom 的常驻成本。
 *  clear/replace 回退全量重算，与 keys() 一致（README 支持矩阵「clear/replace 部分重算」）。
 */
function createEntryList<K, V, R>(source: RxMap<K, V>, snapshot: () => R[], project: (key: K, value: V) => R): RxList<R> {
    // 与 source.data 迭代序逐条同步的 key 序
    let order: K[] = []
    // CAUTION key → 位置**上界**（惰性建立）：已有 key 的位置只会因更靠前的删除而左移
    //  （新 key 恒追加在尾部、重新 set 已有 key 不改变 Map 迭代序），所以从上界向左找到的
    //  第一处命中就是当前位置。队首删除（逐出最旧 key，FIFO）让其后所有 key 整体左移一位，
    //  用一个全局计数 frontRemoved 记账（上界以「位置 + 记录时的 frontRemoved」存放），
    //  纯 FIFO / 纯尾部增删下上界恒精确；只有发生在某 key 之前的**非队首**删除才需要它
    //  向左补扫，代价 = 这类删除自上次定位以来的次数（总量不超过旧实现在每次删除时为其后
    //  每一行重写 index atom 的次数）。更新已有 key 的值因此保持 O(1)——线性扫描会把
    //  逐 key 更新大 Map 变成 O(n²)。
    let hints: Map<K, number> | undefined
    let frontRemoved = 0
    // SameValueZero：与 Map 的 key 语义一致（NaN 可定位，见 keys() 的同款说明）
    const sameKey = (a: K, b: K) => a === b || (a !== a && b !== b)
    const locate = (key: K): number => {
        if (hints === undefined) {
            hints = new Map()
            frontRemoved = 0
            for (let i = 0; i < order.length; i++) hints.set(order[i], i)
        }
        const hint = hints.get(key)
        if (hint === undefined) return -1
        let index = Math.min(hint - frontRemoved, order.length - 1)
        while (index >= 0 && !sameKey(order[index], key)) index--
        if (index >= 0) hints.set(key, index + frontRemoved)
        return index
    }
    return new RxList<R>(
        function computation(this: RxList<R>) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
            order = Array.from(source.data.keys())
            hints = undefined
            return snapshot()
        },
        function applyPatch(this: RxList<R>, _data: Atom<R[]>, triggerInfos) {
            for (const info of triggerInfos) {
                if (info.method === 'set') {
                    const key = normalizeKey(info.argv![0] as K)
                    const value = info.argv![1] as V
                    const [hasValue] = info.methodResult as [boolean, V]
                    if (!hasValue) {
                        if (hints !== undefined) hints.set(key, order.length + frontRemoved)
                        order.push(key)
                        this.push(project(key, value))
                    } else {
                        const index = locate(key)
                        // 记账与 info 对不上（不应发生）：回退全量重算自愈
                        if (index === -1) return false
                        this.set(index, project(key, value))
                    }
                } else if (info.method === 'delete') {
                    // 原始 key（可能是 -0）可直接定位：hints（Map）与 sameKey（===）对 ±0 都成立
                    const key = info.argv![0] as K
                    const index = locate(key)
                    if (index === -1) return false
                    order.splice(index, 1)
                    if (index === 0) frontRemoved++
                    hints!.delete(key)
                    this.splice(index, 1)
                } else {
                    // clear / replace
                    return false
                }
            }
        }
    )
}
/**
 * @category Basic
 */
export class RxMap<K, V> extends Computed{
    data!: Map<K, V>
    trackClassInstance = true
    // CAUTION 不用参数属性(见 RxList 构造器说明,base 已条件赋值);
    //  曾有的第 6 参 forceAtom 全仓与下游零消费,死参数已移除。
    constructor(sourceOrGetter?: EntryType|PlainObjectType|null|GetterType, applyPatch?: ApplyPatchType, scheduleRecompute?: DirtyCallback, callbacks? : CallbacksType, skipIndicator? : SkipIndicator) {
        const getter = typeof sourceOrGetter === 'function' ? sourceOrGetter as GetterType : undefined
        const source = typeof sourceOrGetter === 'function' ? undefined : sourceOrGetter
        // 自己可能是 computed，也可能是最初的 reactive
        super(getter, applyPatch, scheduleRecompute, callbacks, skipIndicator)
        // 自己是 source
        // CAUTION 架构语义（AGENTS.md A3）：传入 Map 时直接采纳引用（所有权移交），
        //  之后必须通过本实例的方法修改。刻意不做防御性拷贝，明确不修。
        if (source) {
            this.data = isMap(source) ? source : new Map(Array.isArray(source) ? source : Object.entries(source))
        } else {
            this.data = new Map()
        }

        if (this.getter) {
            this.run([], true)
        }
    }
    replace = (source: EntryType|PlainObjectType|Map<K,V>) => {
        // 已销毁实例的变更是 no-op（复活写入防线，见 RxList.spliceArray 的说明）
        if (!this.active) {
            warn('mutating a destroyed RxMap is a no-op')
            return
        }
        let entries: EntryType

        const oldKeys = new Set(this.data.keys())
        if (source instanceof Map) {
            entries = Array.from(source.entries())
        } else {
            entries = Array.isArray(source) ? source : Object.entries(source)
        }

        entries.forEach(([key, value]) => {
            const hasValue = this.data.has(key)
            const oldValue = hasValue ? this.data.get(key) : undefined
            this.data.set(key, value)
            if (hasValue) {
                // CAUTION 与 set() 的判等门一致（Object.is，见 set 的说明）：replace
                //  是"set 的批量形态"，值未变的 key 不触发 SET——否则 get(key) 的
                //  订阅者在整表 replace 时被全量幽灵触发。SET info 同样带 oldValue,
                //  与 set() 的协议形状一致。
                if (!Object.is(value, oldValue)) {
                    this.trigger(this, TriggerOpTypes.SET, { key, newValue: value, oldValue})
                }
            } else {
                this.trigger(this, TriggerOpTypes.ADD, { key, newValue: value})
            }
            oldKeys.delete(key)
        })

        const deleteEntries: [K, V][] = []
        oldKeys.forEach((key, value) => {
            const oldValue = this.data.get(key)!
            this.data.delete(key)
            this.trigger(this, TriggerOpTypes.DELETE, { key, oldValue})
            deleteEntries.push([key, oldValue])
        })

        this.trigger(this, TriggerOpTypes.METHOD, {method: 'replace', argv: [source], methodResult: deleteEntries})
        this.sendTriggerInfos()
    }
    replaceData = this.replace

    // set methods
    set(key: K, value: V) {
        if (!this.active) {
            warn('mutating a destroyed RxMap is a no-op')
            return
        }
        // CAUTION 载荷透传调用方的原始 key（含 -0）：triggerInfo/argv 是 axii/axle 消费的外部
        //  协议，不在写入口改写（AGENTS「内部消费者应独立归一化」）。容器（原生 Map）存的是 +0，
        //  按 argv 重放的内部消费者（keys()/values()/entries()）各自用 normalizeKey 归一化。
        const hasValue = this.data.has(key)
        const oldValue = this.data.get(key)
        this.data.set(key, value)
        if (hasValue) {
            // CAUTION Object.is 而不是 ===（与 atom 判等、toSorted 身份定位的库内
            //  语义一致）：0 → -0 是可观察变化（此刻 data 已写入 -0），=== 会静默
            //  吞掉触发导致派生 values/entries 与 source 分叉；NaN → NaN 无变化，
            //  === 却会重复触发两次。
            if (Object.is(value, oldValue)) return

            this.trigger(this, TriggerOpTypes.SET, { key, newValue: value, oldValue})
        } else {
            this.trigger(this, TriggerOpTypes.ADD, { key, newValue: value})
        }
        this.trigger(this, TriggerOpTypes.METHOD, { method: 'set', argv: [key, value], methodResult: [hasValue, oldValue]})

        this.sendTriggerInfos()
    }

    delete(key: K) {
        if (!this.active) {
            warn('mutating a destroyed RxMap is a no-op')
            return
        }
        // 载荷透传原始 key（理由见 set）
        const hasValue = this.data.has(key)
        let oldValue:V|undefined
        if (hasValue) {
            oldValue = this.data.get(key)
            this.data.delete(key)
            this.trigger(this, TriggerOpTypes.DELETE, { key, newValue: undefined, oldValue})

            this.trigger(this, TriggerOpTypes.METHOD, { method: 'delete', argv: [key], methodResult: oldValue})

            this.sendTriggerInfos()
        }
    }

    clear() {
        if (!this.active) {
            warn('mutating a destroyed RxMap is a no-op')
            return
        }
        const entries = Array.from(this.data.entries())
        this.data.clear()
        entries.forEach(([key, value]) => {
            this.trigger(this, TriggerOpTypes.DELETE, { key,  oldValue: value})
        })
        this.trigger(this, TriggerOpTypes.METHOD, { method: 'clear', methodResult: entries})
        this.sendTriggerInfos()
    }

    // track methods
    get(key: K) {
        // 先执行 track 才会触发 recompute
        notifier.track(this, TrackOpTypes.GET, key)
        return this.data.get(key)
    }
    forEach(handler: (item: V, index: K) => void) {
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)

        for(let [key, value ] of this.data) {
            handler(value!, key)
        }
        // track iterator
    }
    [Symbol.iterator](): IterableIterator<[K, V]> {
        // 与 forEach 一致：只 track ITERATE_KEY（set/delete/add 的变更路径都会通知它），
        // 直接用原生 Map 迭代器，不再快照 keys 数组、也不逐 key track GET
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
        return this.data[Symbol.iterator]()
    }
    // CAUTION keys/values/entries/size 全部惰性创建：旧实现每个 RxMap 构造时无条件
    //  预建 1 个 keys RxList + 2 个 map 派生 RxList + 1 个 size computed（约 5 个
    //  Computed 的固定成本），groupBy 这类每 group 一个 RxMap 的场景按倍数放大。
    //  createDetached 解决旧注释里"在 autorun 中读会被当作 children 误销毁"的问题。
    declare _keys?: RxList<K>
    declare _values?: RxList<V>
    declare _entries?: RxList<[K, V]>
    declare _size?: Atom<number>
    // size 的内部 Computed：判活用（atom 本身不带 active），与 _size 同时赋值
    /** @internal */
    declare _sizeEffect?: Computed
    // CAUTION 缓存只在它还活着时复用：meta 是按需创建、全体使用方共享的单例，而 README §6
    //  让使用方对派生结构调用 destroy()——keys()/values() 在调用点上与 list.map() 无从区分。
    //  使用方销毁了它之后若继续返回这个缓存，本 Map 的所有读者此后拿到的都是冻结在销毁
    //  时刻的快照（静默陈旧，且再也换不回活的）。四个 meta 统一：宿主还活着而缓存已死 →
    //  下次读取重建（旧实例保持已销毁，新实例随宿主销毁）；宿主已销毁 → 继续返回同一个
    //  已销毁快照（幂等，见"已销毁结构的 meta 首读"）。RxSet.size / RxList.length 同规则。
    keys(): RxList<K> {
        if (this._keys && (this._keys.active || !this.active)) return this._keys
        this._keys = ReactiveEffect.createDetached(() => {
            const source = this
            return new RxList<K>(
                function computation(this: RxList<K>) {
                    this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD);
                    return Array.from(source.data.keys())
                },
                function applyPatch(this: RxList<K>, data: Atom<K[]>, triggerInfos){
                    for(let info of triggerInfos) {
                        if (info.type === TriggerOpTypes.METHOD) {
                            if (info.method === 'clear' || info.method === 'replace') {
                                return false
                            } else if (info.method === 'set') {
                                const [hasValue] = info.methodResult as [boolean, V]
                                if (!hasValue) {
                                    // 载荷是原始 key（可能是 -0），列表里放容器存的 key
                                    this.push(normalizeKey(info.argv![0] as K))
                                }
                            } else if(info.method === 'delete') {
                                // CAUTION SameValueZero 查找：Map 的 key 语义支持 NaN，
                                //  indexOf 的严格相等找不到 NaN 会返回 -1，直接 splice(-1, 1)
                                //  会按负 index 归一化误删最后一个 key。
                                const deletedKey = info.argv![0] as K
                                const index = this.data.findIndex(
                                    key => key === deletedKey || (key !== key && deletedKey !== deletedKey)
                                )
                                if (index !== -1) this.splice(index, 1)
                            } else {
                                assert(false, 'unreachable')
                            }
                        } else {
                            assert(false, 'unreachable')
                        }
                    }
                }
            )
        })
        // 已销毁结构的 meta 首读:快照值保留,立即随葬(等价类说明见 RxList.length)
        if (!this.active) this._keys.destroy()
        return this._keys
    }
    values(): RxList<V> {
        if (this._values && (this._values.active || !this.active)) return this._values
        this._values = ReactiveEffect.createDetached(() => createEntryList<K, V, V>(
            this,
            () => Array.from(this.data.values()),
            (_key, value) => value
        ))
        if (!this.active) this._values.destroy()
        return this._values
    }
    entries(): RxList<[K, V]> {
        if (this._entries && (this._entries.active || !this.active)) return this._entries
        this._entries = ReactiveEffect.createDetached(() => createEntryList<K, V, [K, V]>(
            this,
            () => Array.from(this.data.entries()),
            (key, value) => [key, value]
        ))
        if (!this.active) this._entries.destroy()
        return this._entries
    }
    get size(): Atom<number> {
        if (this._size && (this._sizeEffect!.active || !this.active)) return this._size
        this._size = ReactiveEffect.createDetached(() => {
            const source = this
            return computed(
                function computation(this: Computed) {
                    this.manualTrack(source, TrackOpTypes.ITERATE, ITERATE_KEY)
                    return source.data.size
                },
                function applyPatch(this: Computed, data: Atom<number>) {
                    data(source.data.size)
                }
            )
        })
        this._sizeEffect = getComputedInternal(this._size)!
        if (!this.active) destroyComputed(this._size)
        return this._size
    }
    /**
     * @internal
     * 统一资源清理钩子（见 ReactiveEffect.destroyResources）。
     */
    destroyResources() {
        // CAUTION 只销毁真正创建过的派生结构（getter 惰性，直接访问会先创建再销毁）。
        //  四个 meta 各自直接订阅本 Map，彼此独立，销毁顺序无关。
        // CAUTION 每一步都可能执行用户代码（meta 的 destroy 监听者、它们的下游级联、基类钩子里
        //  本结构自己的 context.onCleanup），任何一步抛错都不能让后面的步骤被跳过（「销毁必须做完」，
        //  README §6）：曾是顺序执行，values() 的销毁一抛错，entries()/keys()/size 就一直活着、
        //  一直订阅着本 Map，基类钩子（context.onCleanup、cleanPromise 的 settle）也整个跳过。
        //  规则与 RxList.destroyResources、静态销毁核心一致：全部做完，第一个错误抛给调用方，
        //  其后的错误走 reportTeardownError。
        let failed = false
        let first: unknown
        const record = (error: unknown) => {
            if (failed) {
                reportTeardownError(error)
            } else {
                failed = true
                first = error
            }
        }
        for (const meta of [this._values, this._entries, this._keys]) {
            if (!meta) continue
            try {
                meta.destroy()
            } catch (error) {
                record(error)
            }
        }
        if (this._size) {
            try {
                destroyComputed(this._size)
            } catch (error) {
                record(error)
            }
        }
        try {
            super.destroyResources()
        } catch (error) {
            record(error)
        }
        if (failed) throw first
    }
}