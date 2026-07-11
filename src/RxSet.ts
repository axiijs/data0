import {ApplyPatchType, CallbacksType, computed, Computed, destroyComputed, DirtyCallback, GetterType} from "./computed.js";
import {Atom} from "./atom.js";
import {ITERATE_KEY, notifier, TriggerInfo} from "./notify.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {ReactiveEffect} from "./reactiveEffect.js";
import {RxList} from "./RxList";
/**
 * @category Basic
 */
export class RxSet<T> extends Computed {
    data!: Set<T>
    trackClassInstance = true

    constructor(sourceOrGetter?: T[]|null|GetterType, public applyPatch?: ApplyPatchType, scheduleRecompute?: DirtyCallback, public callbacks? : CallbacksType) {
        const getter = typeof sourceOrGetter === 'function' ? sourceOrGetter : undefined
        const source = typeof sourceOrGetter !== 'function' ? sourceOrGetter : undefined

        // 自己可能是 computed，也可能是最初的 reactive
        super(getter, applyPatch, scheduleRecompute, callbacks)
        this.getter = getter

        // 自己是 source
        this.data = source instanceof Set ? source : new Set(Array.isArray(source) ? source : [])

        if (this.getter) {
            this.run([], true)
        }
    }
    replaceData(newData: T[]|Set<T>) {
        return this.replace(newData)
    }

    replace(newData: T[]|Set<T>): [T[], T[]]{
        const old = this.data
        this.data = newData instanceof Set ? newData : new Set(newData)

        const newItems: T[] = []
        const deletedItems: T[] = []

        old.forEach((value) => {
            if(!this.data.has(value)) {
                this.trigger(this, TriggerOpTypes.DELETE, { key: value, oldValue: value})
                deletedItems.push(value)
            }
        });

        [...newData].forEach((value) => {
            if(!old.has(value)) {
                this.trigger(this, TriggerOpTypes.ADD, { key: value, newValue: value})
                newItems.push(value)
            }
        })

        this.trigger(this, TriggerOpTypes.METHOD, { method: 'replace', argv: [newData], methodResult: [newItems, deletedItems]})
        this.sendTriggerInfos()
        return [newItems, deletedItems]
    }

    // 显式 set 某一个 index 的值
    add(value: T) {
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
        this.data.forEach(handler)
        notifier.track(this, TrackOpTypes.ITERATE, ITERATE_KEY)
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
                        this.push(x)
                    })

                    deletedItems.forEach(x => {
                        // CAUTION SameValueZero 查找：Set 的成员语义支持 NaN，indexOf 的
                        //  严格相等找不到 NaN 会返回 -1，splice(-1, 1) 会误删最后一个元素。
                        const index = this.data.findIndex(item => item === x || (item !== item && x !== x))
                        if (index !== -1) this.splice(index, 1)
                    })
                })
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
        return this._size ?? (this._size = ReactiveEffect.createDetached(() => {
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
        }))
    }
    destroy() {
        // 只销毁真正创建过的 size（getter 惰性，直接访问会先创建再销毁）
        if (this._size) destroyComputed(this._size)
        super.destroy()
    }
}


