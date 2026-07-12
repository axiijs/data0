import {describe, expect, test} from 'vitest'
import {Computed} from '../src/computed.js'
import {TriggerInfo} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {RxList, ReorderPatchInfo} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'

/**
 * 方法 13:消费者契约回放(consumer contract pinning)。
 *
 * axii(RxListHost.handleSplice/handleReorder)与 axle(同名宿主)在 data0 仓库外
 * 直接消费 triggerInfo 的字段形状:
 *   - splice:`argv` 按用户原始参数透传(start 可为负/小数/NaN/undefined,
 *     消费方自行归一化)、`argv.slice(2)` 为新项、`methodResult` 为删除项数组;
 *   - reorder:`argv[0]` 为 [oldIndex, newIndex][]、`reorderInfo.kind/affectedRange/
 *     movedCount/oldIndexToNewIndex`(reposition/swap 另有 start/newStart/limit);
 *   - EXPLICIT_KEY_CHANGE(set):`key` 原样透传(含越界)、`newValue`/`oldValue`、
 *     `methodResult` 为被替换的旧值;
 *   - RxSet:add/delete 的 `argv[0]`,replace 的 `methodResult` 为 [newItems, deletedItems];
 *   - RxMap:set 的 `methodResult` 为 [hasValue, oldValue],delete 的 `methodResult`
 *     为旧值,clear 的 `methodResult` 为 entries 快照,replace 的 `methodResult` 为删除 entries。
 *
 * 本文件把这些**下游锁定的协议形状**钉进 data0 自己的 CI:任何改变字段形状/语义的
 * 修改在合并前当场失败,而不是等 axii/axle 升级时才发现断链。
 * (协议变更必须与下游同步;见 AGENTS.md「argv 原始参数契约」检查项。)
 */

function captureListInfos(source: RxList<any>): {infos: TriggerInfo[], destroy: () => void} {
    const infos: TriggerInfo[] = []
    const c = new Computed(
        function (this: Computed) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
        },
        function (this: Computed, _data, triggerInfos: TriggerInfo[]) {
            infos.push(...triggerInfos)
        },
    )
    c.run([], true)
    return {infos, destroy: () => c.destroy()}
}

function captureMethodInfos(source: any): {infos: TriggerInfo[], destroy: () => void} {
    const infos: TriggerInfo[] = []
    const c = new Computed(
        function (this: Computed) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
        },
        function (this: Computed, _data, triggerInfos: TriggerInfo[]) {
            infos.push(...triggerInfos)
        },
    )
    c.run([], true)
    return {infos, destroy: () => c.destroy()}
}

describe('RxList 变更方法的 info 协议形状(axii/axle RxListHost 消费面)', () => {
    test('splice:argv 原始透传(负/小数/NaN 不归一化),methodResult 为删除项', () => {
        const list = new RxList<number>([1, 2, 3, 4])
        const {infos, destroy} = captureListInfos(list)
        try {
            list.splice(-2.5, NaN, 9)
            expect(infos.length).toBe(1)
            const info = infos[0]
            expect(info.method).toBe('splice')
            expect(info.type).toBe(TriggerOpTypes.METHOD)
            // argv[0]/argv[1] 必须是用户原始参数(axle 明确依赖并自行归一化)
            expect(Object.is(info.argv![0], -2.5)).toBe(true)
            expect(Number.isNaN(info.argv![1] as number)).toBe(true)
            expect(info.argv!.slice(2)).toEqual([9])
            // -2.5 → trunc -2 → 长度4 回退 index 2;NaN deleteCount → 0
            expect(info.methodResult).toEqual([])
            expect(list.data).toEqual([1, 2, 9, 3, 4])

            list.splice(0, 2)
            const second = infos[1]
            expect(second.methodResult).toEqual([1, 2]) // 删除项数组
        } finally {
            destroy()
            list.destroy()
        }
    })

    test('push/pop/shift/unshift 均以 splice 形状透出(操作时位置)', () => {
        const list = new RxList<number>([1, 2])
        const {infos, destroy} = captureListInfos(list)
        try {
            list.push(3)
            expect(infos[0].method).toBe('splice')
            expect(infos[0].argv).toEqual([2, 0, 3])
            list.pop()
            expect(infos[1].argv).toEqual([2, 1])
            expect(infos[1].methodResult).toEqual([3])
            list.shift()
            expect(infos[2].argv).toEqual([0, 1])
            expect(infos[2].methodResult).toEqual([1])
            list.unshift(0)
            expect(infos[3].argv).toEqual([0, 0, 0])
        } finally {
            destroy()
            list.destroy()
        }
    })

    test('set:EXPLICIT_KEY_CHANGE 的 key 原样透传(含越界),methodResult 为旧值', () => {
        const list = new RxList<number>([1, 2])
        const {infos, destroy} = captureListInfos(list)
        try {
            list.set(1, 9)
            // set 同时透出 SET 与 EXPLICIT_KEY_CHANGE;宿主消费 EXPLICIT_KEY_CHANGE
            const ekc = infos.find(i => i.type === TriggerOpTypes.EXPLICIT_KEY_CHANGE)!
            expect(ekc.key).toBe(1)
            expect(ekc.newValue).toBe(9)
            expect(ekc.oldValue).toBe(2)
            expect(ekc.methodResult).toBe(2)

            infos.length = 0
            list.set(6, 42) // 越界:key 原样透传,由下游拒绝或归一化(README 契约)
            const oob = infos.find(i => i.type === TriggerOpTypes.EXPLICIT_KEY_CHANGE)!
            expect(oob.key).toBe(6)
            expect(oob.oldValue).toBe(undefined)
        } finally {
            destroy()
            list.destroy()
        }
    })

    test('reorder 家族:argv[0] 为 Order 对,reorderInfo 的 kind/affectedRange/movedCount/映射', () => {
        const list = new RxList<number>([3, 1, 2])
        const {infos, destroy} = captureListInfos(list)
        try {
            list.sortSelf((a, b) => a - b)
            const sortInfo = infos[0]
            expect(sortInfo.method).toBe('reorder')
            expect(Array.isArray(sortInfo.argv![0])).toBe(true)
            const ri = sortInfo.reorderInfo as ReorderPatchInfo
            expect(ri.kind).toBe('sort')
            expect(ri.affectedRange).toEqual([0, 2])
            expect(ri.movedCount).toBeGreaterThan(0)
            expect(ri.oldIndexToNewIndex.get(0)).toBe(2) // 3 从 0 挪到 2

            infos.length = 0
            list.reposition(0, 2) // [1,2,3] → [2,3,1]
            const moveInfo = infos[0].reorderInfo as ReorderPatchInfo
            expect(moveInfo.kind).toBe('move')
            expect(moveInfo.start).toBe(0)
            expect(moveInfo.newStart).toBe(2)
            expect(moveInfo.limit).toBe(1)

            infos.length = 0
            list.swap(0, 2)
            const swapInfo = infos[0].reorderInfo as ReorderPatchInfo
            expect(swapInfo.kind).toBe('swap')
            expect(swapInfo.affectedRange).toEqual([0, 2])
        } finally {
            destroy()
            list.destroy()
        }
    })
})

describe('RxSet 变更方法的 info 协议形状(selection/派生运算消费面)', () => {
    test('add/delete 的 argv[0],replace 的 methodResult=[newItems, deletedItems]', () => {
        const set = new RxSet<number>([1, 2])
        const {infos, destroy} = captureMethodInfos(set)
        try {
            set.add(3)
            expect(infos[0].method).toBe('add')
            expect(infos[0].argv).toEqual([3])
            set.delete(1)
            expect(infos[1].method).toBe('delete')
            expect(infos[1].argv).toEqual([1])
            set.replace([2, 4, 4]) // 含重复入参:newItems 必须按 Set 语义去重
            expect(infos[2].method).toBe('replace')
            const [newItems, deletedItems] = infos[2].methodResult as [number[], number[]]
            expect(newItems).toEqual([4])
            expect(deletedItems).toEqual([3])
        } finally {
            destroy()
            set.destroy()
        }
    })
})

describe('RxMap 变更方法的 info 协议形状(keys/values/entries 消费面)', () => {
    test('set 的 methodResult=[hasValue, oldValue],delete 为旧值,clear 为 entries,replace 为删除 entries', () => {
        const map = new RxMap<string, number>({a: 1})
        const {infos, destroy} = captureMethodInfos(map)
        try {
            map.set('b', 2)
            expect(infos[0].method).toBe('set')
            expect(infos[0].argv).toEqual(['b', 2])
            expect(infos[0].methodResult).toEqual([false, undefined])
            map.set('a', 9)
            expect(infos[1].methodResult).toEqual([true, 1])
            map.delete('a')
            expect(infos[2].method).toBe('delete')
            expect(infos[2].methodResult).toBe(9)
            map.clear()
            expect(infos[3].method).toBe('clear')
            expect(infos[3].methodResult).toEqual([['b', 2]])
            map.replace({x: 1})
            expect(infos[4].method).toBe('replace')
            expect(infos[4].methodResult).toEqual([]) // 清空后无删除 entries
        } finally {
            destroy()
            map.destroy()
        }
    })
})
