/**
 * RxList 的 selection 家族(createSelection / createSelections /
 * createIndexKeySelection 及其 inner 记账机制)。
 *
 * 2026-H3 round6 工程面拆分:从 2600+ 行的 RxList.ts 中按既有的自然模块边界
 * (本组是独立函数而非类方法)拆出,行为零变化;RxList.ts 原位 re-export,
 * 公开导入路径(`data0` / `./RxList`)不变。
 *
 * CAUTION 与 RxList.ts 构成运行时循环依赖(与 RxList↔RxSet 同款,安全前提:
 *  双方都只在**函数体内**使用对方,模块顶层零求值依赖)。共享守卫
 *  (isDenseIndexKey/sameValueZero)从 RxList.ts 导入,保持单一定义。
 */
import {Atom, atom, isAtom} from "./atom.js";
import {Computed} from "./computed.js";
import {TriggerInfo} from "./notify.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {normalizeSpliceStart} from "./util.js";
import {isDenseIndexKey, RxList, sameValueZero} from "./RxList.js";
import type {Order, ReorderPatchInfo} from "./RxList.js";
import type {RxSet} from "./RxSet.js";

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

    // CAUTION atom 单选分支的判等必须是 SameValueZero(2026-H3 round6 R6-1):
    //  记账 Map(itemToIndicators)与 RxSet 多选分支天然是 SameValueZero,atom
    //  分支曾用 ===——NaN item 下「增量置 indicator(Map.get 命中)vs 全量重建
    //  (=== 不命中)」分叉,autoReset 也回收不了 NaN 选中值。判等门必须覆盖
    //  同一语义的全部入口(全量重建/增量更新/autoReset 回收,方法 18/19 规则)。
    function createNewIndicator(item:T) {
        const indicator = atom(isAtom(currentValues) ? sameValueZero(currentValues.raw, item) : currentValues.data.has(item))
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
            // SameValueZero 与 createNewIndicator/记账 Map 对齐(R6-1):===
            // 会让已删除的 NaN 选中值永不回收(RxSet 分支的 Set.has 可回收)。
            if (sameValueZero(item, currentValues.raw)) {
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

