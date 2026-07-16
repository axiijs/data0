import {ApplyPatchType, CallbacksType, computed, Computed, destroyComputed, DirtyCallback, GetterType} from "./computed.js";
import {Atom} from "./atom.js";
import {ITERATE_KEY, notifier, TriggerInfo} from "./notify.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {ReactiveEffect} from "./reactiveEffect.js";
import {toProtocolPayload, warn} from "./util.js";
import {RxList} from "./RxList";
/**
 * @category Basic
 */
export class RxSet<T> extends Computed {
    data!: Set<T>
    trackClassInstance = true

    // 不用参数属性(见 RxList 构造器说明,base 已条件赋值)
    constructor(sourceOrGetter?: T[]|null|GetterType, applyPatch?: ApplyPatchType, scheduleRecompute?: DirtyCallback, callbacks? : CallbacksType) {
        const getter = typeof sourceOrGetter === 'function' ? sourceOrGetter : undefined
        const source = typeof sourceOrGetter !== 'function' ? sourceOrGetter : undefined

        // 自己可能是 computed，也可能是最初的 reactive
        super(getter, applyPatch, scheduleRecompute, callbacks)

        // 自己是 source
        // CAUTION 架构语义（AGENTS.md A3）：传入 Set 时直接采纳引用（所有权移交），
        //  之后必须通过本实例的方法修改。刻意不做防御性拷贝，明确不修。
        this.data = source instanceof Set ? source : new Set(Array.isArray(source) ? source : [])

        if (this.getter) {
            this.run([], true)
        }
    }
    replaceData(newData: T[]|Set<T>) {
        return this.replace(newData)
    }

    replace(newData: T[]|Set<T>): [T[], T[]]{
        // 已销毁实例的变更是 no-op（复活写入防线，见 RxList.spliceArray 的说明）
        if (!this.active) {
            warn('mutating a destroyed RxSet is a no-op')
            return [[], []]
        }
        const old = this.data
        // CAUTION 架构语义（AGENTS.md A3）：传入 Set 时直接采纳引用（所有权移交），
        //  调用方之后复用该 Set 直接增删不会触发任何通知。
        this.data = newData instanceof Set ? newData : new Set(newData)

        const newItems: T[] = []
        const deletedItems: T[] = []

        old.forEach((value) => {
            if(!this.data.has(value)) {
                this.trigger(this, TriggerOpTypes.DELETE, { key: value, oldValue: value})
                deletedItems.push(value)
            }
        });

        // CAUTION 新增项必须基于采纳后的 Set（SameValueZero 去重）而不是原始入参：
        //  数组含重复值（[2,2]）时按数组遍历会触发重复 ADD，methodResult.newItems
        //  含重复项，toList 等按事件重放的派生结构会出现重复行。
        this.data.forEach((value) => {
            if(!old.has(value)) {
                this.trigger(this, TriggerOpTypes.ADD, { key: value, newValue: value})
                newItems.push(value)
            }
        })

        // 载荷持内层数组的独立副本(所有权契约,见 util.toProtocolPayload):
        // 返回的 [newItems, deletedItems] 归调用方,延迟消费窗口里改写不毒化 patch 消费者。
        this.trigger(this, TriggerOpTypes.METHOD, { method: 'replace', argv: [newData], methodResult: [toProtocolPayload(newItems), toProtocolPayload(deletedItems)]})
        this.sendTriggerInfos()
        return [newItems, deletedItems]
    }

    // 显式 set 某一个 index 的值
    add(value: T) {
        if (!this.active) {
            warn('mutating a destroyed RxSet is a no-op')
            return this
        }
        if (!this.data.has(value)) {
            this.data.add(value)
            this.trigger(this, TriggerOpTypes.ADD, { key: value, newValue: value})
            this.trigger(this, TriggerOpTypes.METHOD, { method: 'add', argv: [value]})
            this.sendTriggerInfos()
        }
        return this
    }
    clear() {
        return this.replace([])
    }
    delete(value:T) {
        if (!this.active) {
            warn('mutating a destroyed RxSet is a no-op')
            return this
        }
        if (this.data.has(value)) {
            this.data.delete(value)
            this.trigger(this, TriggerOpTypes.DELETE, { key: value, argv: [value]})
            this.trigger(this, TriggerOpTypes.METHOD, { method: 'delete', argv: [value]})
            this.sendTriggerInfos()
        }
        return this
    }
    has(value:T): Atom<boolean> {
        const base = this
        //  has 是 n(1) 的操作，所以不用 applyPatch 了。
        return computed(() => {
            notifier.track(base, TrackOpTypes.ITERATE, ITERATE_KEY)
            return base.data.has(value)
        })
    }
    // 在当前 set 里，但不在 other set 里
    difference(other: RxSet<T>): RxSet<T> {
        const base = this

        return new RxSet(
            function computation(this: RxSet<T>) {
                this.manualTrack(base, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(other, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                // 直接迭代 Set，省去 [...] 的中间数组物化
                const result = new Set<T>()
                for (const x of base.data) {
                    if (!other.data.has(x)) result.add(x)
                }
                return result
            },
            function applyPatch(this: RxSet<T>, data:any, triggerInfos: TriggerInfo[]) {
                triggerInfos.forEach(({ methodResult, method, argv, newValue, source, result}) => {
                    let newItems: T[] = []
                    let deletedItems: T[] = []
                    if (method === 'add')  {
                        newItems = [argv![0]]
                    } else if (method === 'delete') {
                        deletedItems = [argv![0]]
                    } else {
                        // 只支持 replace method
                        [newItems, deletedItems] = methodResult as [T[], T[]]
                    }

                    if(source === base) {
                        newItems.forEach(x => {
                            if (!other.data.has(x)) {
                                this.add(x)
                            }
                        })

                        deletedItems.forEach(x => {
                            this.delete(x)
                        })
                    } else {
                        newItems.forEach(x => {
                            this.delete(x)
                        })

                        deletedItems.forEach(x => {
                            if(base.data.has(x)) {
                                this.add(x)
                            }
                        })
                    }
                })
            }
        )
    }
    intersection(other: RxSet<T>): RxSet<T> {
        const base = this

        return new RxSet(
            function computation(this: RxSet<T>) {
                this.manualTrack(base, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(other, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                const result = new Set<T>()
                for (const x of base.data) {
                    if (other.data.has(x)) result.add(x)
                }
                return result
            },
            function applyPatch(this: RxSet<T>, data:any, triggerInfos: TriggerInfo[]) {
                triggerInfos.forEach(({type, method, methodResult, argv, newValue, source, result}) => {
                    let newItems: T[] = []
                    let deletedItems: T[] = []
                    if (method === 'add')  {
                        newItems = [argv![0]]
                    } else if (method === 'delete') {
                        deletedItems = [argv![0]]
                    } else {
                        // 只支持 replace method
                        [newItems, deletedItems] = methodResult as [T[], T[]]
                    }

                    newItems.forEach(x => {
                        const toCheck = source === base ? other : base
                        if (toCheck.data.has(x)) {
                            this.add(x)
                        }
                    })

                    deletedItems.forEach(x => {
                        this.delete(x)
                    })
                })
            }
        )
    }
    // 差集
    symmetricDifference(other: RxSet<T>): RxSet<T> {
        const base = this

        return new RxSet(
            function computation(this: RxSet<T>) {
                this.manualTrack(base, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(other, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                const result = new Set<T>()
                for (const x of base.data) {
                    if (!other.data.has(x)) result.add(x)
                }
                for (const x of other.data) {
                    if (!base.data.has(x)) result.add(x)
                }
                return result
            },
            function applyPatch(this: RxSet<T>, data:any, triggerInfos: TriggerInfo[]) {
                triggerInfos.forEach(({methodResult, method, argv, newValue, source, result}) => {
                    let newItems: T[] = []
                    let deletedItems: T[] = []
                    if (method === 'add')  {
                        newItems = [argv![0]]
                    } else if (method === 'delete') {
                        deletedItems = [argv![0]]
                    } else {
                        // 只支持 replace method
                        [newItems, deletedItems] = methodResult as [T[], T[]]
                    }

                    newItems.forEach(x => {
                        const toCheck = source === base ? other : base
                        if (!toCheck.data.has(x)) {
                            this.add(x)
                        } else {
                            this.delete(x)
                        }
                    })

                    deletedItems.forEach(x => {
                        const toCheck = source === base ? other : base
                        if (toCheck.data.has(x)) {
                            this.add(x)
                        } else {
                            this.delete(x)
                        }
                    })
                })
            }
        )
    }
    union(other: RxSet<T>): RxSet<T> {
        const base = this

        return new RxSet(
            function computation(this: RxSet<T>) {
                this.manualTrack(base, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(other, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                const result = new Set<T>(base.data)
                for (const x of other.data) result.add(x)
                return result
            },
            function applyPatch(this: RxSet<T>, data:any, triggerInfos: TriggerInfo[]) {
                triggerInfos.forEach(({methodResult, method, argv, newValue, source, result}) => {

                    let newItems: T[] = []
                    let deletedItems: T[] = []
                    if (method === 'add')  {
                        newItems = [argv![0]]
                    } else if (method === 'delete') {
                        deletedItems = [argv![0]]
                    } else {
                        // 只支持 replace method
                        [newItems, deletedItems] = methodResult as [T[], T[]]
                    }

                    newItems.forEach(x => {
                        this.add(x)
                    })

                    deletedItems.forEach(x => {
                        const toCheck = source === base ? other : base
                        if (!toCheck.data.has(x)) {
                            this.delete(x)
                        } else {
                        }
                    })
                })
            }
        )
    }

    isSubsetOf(other: RxSet<T>): Atom<boolean> {
        const base = this
        const intersection = this.intersection(other)

        return computed(() => {
            return intersection.size() === base.size()
        }, undefined, undefined, {
            onDestroy() {
                intersection.destroy()
            }
        })

    }
    isSupersetOf(other: RxSet<T>): Atom<boolean> {
        return other.isSubsetOf(this)
    }
    isDisjointFrom(other: RxSet<T>): Atom<boolean> {
        const intersection = this.intersection(other)
        return computed(() => {
            return intersection.size() === 0
        }, undefined, undefined, {
            onDestroy() {
                intersection.destroy()
            }
        })
    }
    forEach(handler: (item: T) => void) {
        // CAUTION track 先于迭代(2026-H3 round8 R8-6,与 RxList/RxMap.forEach 对齐):
        //  handler 是用户代码,可能抛错——曾 track 在迭代之后,computed 首算中
        //  handler 抛错时依赖零建立,错误恢复(DIRTY)后源的任何变更都不再触发
        //  本 computed(restoreEffectDeps 恢复的"上一次成功依赖集"是空集),
        //  永久静默陈旧;RxList/RxMap 的同名方法 track 在前,同形态可自愈。
        //  同一 API 的兄弟实现点语义必须一致(R6-1 规则的 forEach 实例)。
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
        this.data.forEach(handler)
    }
    toList(): RxList<T> {
        const base = this
        return new RxList(
            function computation(this: RxList<T>) {
                // 监听 ADD 和 DELETE type
                this.manualTrack(base, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return [...base.data]
            },
            function applyPatch(this: RxList<T>, data:any, triggerInfos: TriggerInfo[]) {
                for (const {method, argv} of triggerInfos) {
                    let newItems: T[] = []
                    let deletedItems: T[] = []
                    if (method === 'add')  {
                        newItems = [argv![0]]
                    } else if (method === 'delete') {
                        deletedItems = [argv![0]]
                    } else {
                        // CAUTION replace 回退全量重算(2026-H3 round7 R7-2):replace 采纳的
                        //  新 Set 决定**全部成员的迭代序**(含存活成员的相对顺序),按
                        //  [newItems, deletedItems] 增量维护只能改成员不能改序——toList 是
                        //  有序 RxList,增量结果与全量重算([...set.data])的顺序分叉,错误
                        //  恢复/force recompute 的重建会让同一集合状态呈现不同行序
                        //  (「增量 ≡ 全量重算」不变量含顺序)。RxMap.keys × replace 的
                        //  先例即回退(兄弟实现点一致性);add/delete 与 Set 插入序天然
                        //  对齐,保持增量。
                        return false
                    }

                    newItems.forEach(x => {
                        this.push(x)
                    })

                    deletedItems.forEach(x => {
                        // CAUTION SameValueZero 查找：Set 的成员语义支持 NaN，indexOf 的
                        //  严格相等找不到 NaN 会返回 -1，splice(-1, 1) 会误删最后一个元素。
                        const index = this.data.findIndex(item => item === x || (item !== item && x !== x))
                        if (index !== -1) this.splice(index, 1)
                    })
                }
            }
        )
    }
    toArray() {
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
        return [...this.data]
    }
    // CAUTION size 惰性创建（createDetached 说明见 RxMap 的同名注释）
    declare _size?: Atom<number>
    get size(): Atom<number> {
        if (this._size) return this._size
        this._size = ReactiveEffect.createDetached(() => {
            const source = this
            return computed(
                function computation(this: Computed) {
                    this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                    return source.data.size
                },
                function applyPatch(this: Computed, data: Atom<number>){
                    data(source.data.size)
                }
            )
        })
        // 已销毁结构的 meta 首读:快照值保留,立即随葬(等价类说明见 RxList.length)
        if (!this.active) destroyComputed(this._size)
        return this._size
    }
    /**
     * @internal
     * 统一资源清理钩子（见 ReactiveEffect.destroyResources）。
     */
    destroyResources() {
        // 只销毁真正创建过的 size（getter 惰性，直接访问会先创建再销毁）
        if (this._size) destroyComputed(this._size)
        super.destroyResources()
    }
}


