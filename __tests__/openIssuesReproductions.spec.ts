import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed} from '../src/computed.js'
import {autorun} from '../src/common.js'
import {batch} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {
    disableData0RetainedObjectDiagnostics,
    enableData0RetainedObjectDiagnostics,
    getData0RetainedObjectDiagnosticsSnapshot,
} from '../src/retainedDiagnostics.js'

/**
 * 2026-07 深度 review 的未修复缺陷证据（AGENTS.md §3：test.fails 临时保存可执行证据；
 * 修复时必须改为普通测试）。所有用例在源码（__DEV__）与生产构建 dist/data0.js 上
 * 均已动态复现。
 *
 * 本轮按 AGENTS.md §3.2 引入的新方法：
 * 7. batch/延迟调度下的多 info 单次 digest 重放差分（此前全部差分 fuzz 都在 batch
 *    外逐操作断言，隐含"每次 digest 恰一条 info"的假设，从未攻击过重放语义）；
 * 8. destroy 僵尸行为横扫 + destroy 事件对称性检查（此前生命周期审计依赖
 *    retainedDiagnostics，而它只统计 active=true 的 effect，源模式结构完全不可见）。
 *
 * 缺陷类 I —— 同一次 digest 重放多条 triggerInfo 时，含 EXPLICIT_KEY_CHANGE 的
 * 序列会被 map/filter/groupBy 用"重放时的终态 source.data"解释"操作时的 key"，
 * 结果与全量重算永久分歧。这落在 AGENTS.md A1/A2 明确划出的"仍属缺陷"边界内
 * （batch 结束后结果与全量重算不一致）。
 * 静态定位：RxList.map 的 explicit key change 分支读 source.data[index]（重放时
 * 终态）；filter 的 beforePatch 前缀计数与 groupBy 的 insertInSourceOrder /
 * removeAtSourcePosition 同样基于终态 source.data 计数。
 */
describe('open issue: batch 中 set+结构操作的派生错乱（等价类：多 info 重放含 EKC）', () => {
    test.fails('map: batch 内 set 后 shift，行值变 NaN', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2)
        try {
            batch(() => {
                source.set(2, 10)   // [1,2,10]
                source.shift()      // [2,10]
            })
            // 实际得到 [4, NaN]
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test.fails('map: batch 内 set 后 unshift，被 set 的行保持旧值', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2)
        try {
            batch(() => {
                source.set(1, 10)   // [1,10,3]
                source.unshift(0)   // [0,1,10,3]
            })
            // 实际得到 [0,2,2,6]，期望 [0,2,20,6]
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test.fails('map(item, index): 同类序列使行 item 变 undefined', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map((item, index) => ({item, i: index}))
        try {
            batch(() => {
                source.set(2, 10)
                source.shift()
            })
            expect(mapped.data.map(e => e.item)).toEqual(source.data)
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test.fails('filter: batch 内 set+unshift 丢失新匹配行', () => {
        const source = new RxList([1, 2, 3])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            batch(() => {
                source.set(1, 10)
                source.unshift(0)
            })
            // 实际得到 [0]，期望 [0, 10]
            expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            filtered.destroy(); source.destroy()
        }
    })

    test.fails('groupBy: batch 内 set+unshift 组内容错乱', () => {
        const source = new RxList([1, 2, 3])
        const grouped = source.groupBy(x => x % 2)
        try {
            batch(() => {
                source.set(1, 10)
                source.unshift(0)
            })
            // 实际组 0 = [0,2,10]（旧值 2 未被替换出组），期望 [0,10]
            expect([...(grouped.data.get(0)?.data ?? [])]).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test.fails('无 batch 也可触发：自定义微任务调度下两次连续写积累重放', async () => {
        const {scheduleNextMicroTask} = await import('../src/computed.js')
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2, {scheduleRecompute: scheduleNextMicroTask as any})
        try {
            source.set(2, 10)
            source.shift()
            await new Promise(r => setTimeout(r, 10))
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })
})

/**
 * 缺陷类 II —— 源模式（无 getter）Rx 结构 active === false，静态
 * ReactiveEffect.destroy 对 inactive 直接 return：destroy 事件从不派发、children
 * 从不清理。filter() 恰好把内部 mapList 的销毁挂在 filtered.on('destroy') 上，
 * 因此 filter 产物的 destroy() 完全无效：僵尸更新 + 对长命 source 的永久订阅泄漏。
 * （retainedDiagnostics 只统计 active=true 的 effect，源模式结构在既有生命周期
 * 审计中完全不可见——这是该缺陷类存活多轮 review 的原因。）
 */
describe('open issue: 源模式结构 destroy 语义（等价类：inactive effect 的销毁路径）', () => {
    test.fails('filter().destroy() 后仍接收更新（僵尸）', () => {
        const source = new RxList([1, 2, 3])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            expect(filtered.data).toEqual([2])
            filtered.destroy()
            source.push(4)
            expect(filtered.data).toEqual([2])
        } finally {
            source.destroy()
        }
    })

    test.fails('filter 反复 create/destroy 在长命 source 上累积活跃 effect', () => {
        enableData0RetainedObjectDiagnostics()
        try {
            const source = new RxList([1, 2, 3, 4, 5])
            const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            for (let i = 0; i < 10; i++) {
                const f = source.filter(x => x % 2 === 0)
                f.destroy()
            }
            const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            source.destroy()
            // 实际累积 110 个活跃 effect（每轮 11 个：mapList + 5 行 computed + 5 indicator）
            expect(after).toBe(baseline)
        } finally {
            disableData0RetainedObjectDiagnostics()
        }
    })

    test.fails('源模式 RxList 的 destroy 事件从不派发（on/callbacks.onDestroy 均失效）', () => {
        const list = new RxList([1])
        let fired = false
        list.on('destroy', () => { fired = true })
        list.destroy()
        expect(fired).toBe(true)
    })
})

/**
 * 缺陷类 III —— 父 effect 重算/销毁通过 destroyChildren → 静态 ReactiveEffect.destroy
 * 销毁 children，绕过子类 destroy 覆写：子 computed 经 context.onCleanup 注册的清理
 * 从不执行（README §6 承诺"随宿主重算/销毁自动清理"）。定时器/事件订阅类资源泄漏。
 * 同路径还绕过 RxList/RxMap/RxSet.destroy 覆写（惰性 meta 销毁）与 settleCleanPromise。
 */
describe('open issue: destroyChildren 绕过子类 destroy 覆写', () => {
    test.fails('子 computed 的 context.onCleanup 在父重算/停止时从不执行', async () => {
        const dep = atom(1)
        const log: string[] = []
        const stop = autorun(() => {
            dep()
            computed(({onCleanup}) => {
                onCleanup(() => log.push('cleanup'))
                return 1
            })
        })
        try {
            dep(2)
            await new Promise(r => setTimeout(r, 5))
        } finally {
            stop()
        }
        // 子 computed 被销毁两次（父重算一次 + stop 一次），清理应执行两次；实际 0 次
        expect(log.length).toBeGreaterThanOrEqual(2)
    })
})

/**
 * 缺陷类 IV —— async applyPatch 挂起期间 destroy 不取消在途 patch：
 * runAsyncPatch/finishPatchRecompute 只对比 recomputeId，不检查 active，
 * destroy 也不推进 recomputeId，patch 恢复后继续改写已销毁实例的数据。
 */
describe('open issue: destroy 不能取消挂起中的 async patch', () => {
    test.fails('destroy 后在途 async patch 恢复执行并继续写入 data', async () => {
        let release: (() => void) | undefined
        const source = new RxList<number>([1])
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            async function applyPatch(this: RxList<number>, _data, infos) {
                await new Promise<void>(resolve => { release = resolve })
                for (const info of infos) {
                    if (info.method === 'splice') {
                        this.spliceArray(this.data.length, 0, info.argv!.slice(2))
                    }
                }
            }
        )
        try {
            source.push(2)
            expect(typeof release).toBe('function')
            derived.destroy()
            const snapshot = derived.data.slice()
            release!()
            await new Promise(r => setTimeout(r, 20))
            expect(derived.data).toEqual(snapshot)
        } finally {
            source.destroy()
        }
    })
})

/**
 * 缺陷类 V —— RxSet.replace 以原始数组（而不是采纳后的 Set）计算 newItems：
 * 数组含重复值时触发重复 ADD，methodResult.newItems 含重复，
 * toList 等按事件重放的派生结构出现重复行。
 */
describe('open issue: RxSet.replace 重复值域', () => {
    test.fails('replace([2,2]) 后 toList 出现重复行', () => {
        const s = new RxSet<number>([1])
        const list = s.toList()
        try {
            s.replace([2, 2])
            expect([...s.data]).toEqual([2])
            expect(list.data).toEqual([2])
        } finally {
            list.destroy()
            s.destroy()
        }
    })
})

/**
 * 缺陷类 VI —— toSorted 相等 key 的 tie 顺序与全量稳定排序分歧：
 * binarySearchInsert 把新元素插到相等区间之后，而全量重算是稳定排序（按源顺序）。
 * 同一份数据的呈现顺序取决于到达历史，违反矩阵"增量 ≡ 全量重算"的差分不变量。
 * （既有 fuzz 用不可区分的 number 值域，观察不到 tie 顺序。）
 */
describe('open issue: toSorted 等值 tie 顺序', () => {
    test.fails('等 key 增量插入与全量稳定排序顺序不一致', () => {
        type Item = {k: number, tag: string}
        const source = new RxList<Item>([{k: 1, tag: 'a'}, {k: 2, tag: 'b'}])
        const sorted = source.toSorted((a, b) => a.k - b.k)
        try {
            source.unshift({k: 1, tag: 'a2'})
            const full = source.data.slice().sort((a, b) => a.k - b.k)
            // 实际 [a, a2, b]，全量稳定排序为 [a2, a, b]
            expect(sorted.data.map(i => i.tag)).toEqual(full.map(i => i.tag))
        } finally {
            sorted.destroy()
            source.destroy()
        }
    })
})
