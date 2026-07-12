import {
    ApplyPatchType,
    CallbacksType,
    computed,
    Computed,
    destroyComputed,
    DirtyCallback,
    GetterType,
    SkipIndicator
} from "./computed.js";
import {ITERATE_KEY, notifier} from "./notify.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {Atom} from "./atom.js";
import {RxList} from "./RxList.js";
import {ReactiveEffect} from "./reactiveEffect.js";
import {assert, isMap, warn} from "./util.js";

type EntryType = [any, any][]
type PlainObjectType = {
    [key: string]: any
}
/**
 * @category Basic
 */
export class RxMap<K, V> extends Computed{
    data!: Map<K, V>
    trackClassInstance = true
    constructor(sourceOrGetter?: EntryType|PlainObjectType|null|GetterType, public applyPatch?: ApplyPatchType, scheduleRecompute?: DirtyCallback, public callbacks? : CallbacksType, public skipIndicator? : SkipIndicator, public forceAtom?: boolean) {
        const getter = typeof sourceOrGetter === 'function' ? sourceOrGetter as GetterType : undefined
        const source = typeof sourceOrGetter === 'function' ? undefined : sourceOrGetter
        // 自己可能是 computed，也可能是最初的 reactive
        super(getter, applyPatch, scheduleRecompute, callbacks, skipIndicator)
        this.getter = getter
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
            this.data.set(key, value)
            if (hasValue) {
                this.trigger(this, TriggerOpTypes.SET, { key, newValue: value})
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
        const hasValue = this.data.has(key)
        const oldValue = this.data.get(key)
        this.data.set(key, value)
        if (hasValue) {
            if (value === oldValue) return

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
    keys(): RxList<K> {
        return this._keys ?? (this._keys = ReactiveEffect.createDetached(() => {
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
                                    this.push(info.argv![0]! as K)
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
        }))
    }
    values(): RxList<V> {
        return this._values ?? (this._values = ReactiveEffect.createDetached(() => this.keys().map(key => this.get(key)!)))
    }
    entries(): RxList<[K, V]> {
        return this._entries ?? (this._entries = ReactiveEffect.createDetached(() => this.keys().map(key => [key, this.get(key)] as [K, V])))
    }
    get size(): Atom<number> {
        return this._size ?? (this._size = ReactiveEffect.createDetached(() => {
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
        }))
    }
    /**
     * @internal
     * 统一资源清理钩子（见 ReactiveEffect.destroyResources）。
     */
    destroyResources() {
        // CAUTION 只销毁真正创建过的派生结构（getter 惰性，直接访问会先创建再销毁）。
        //  values/entries 派生自 keys，先销毁派生者。
        if (this._values) this._values.destroy()
        if (this._entries) this._entries.destroy()
        if (this._keys) this._keys.destroy()
        if (this._size) destroyComputed(this._size)
        super.destroyResources()
    }
}