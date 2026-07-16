import {chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join, resolve} from 'node:path'
import {spawnSync} from 'node:child_process'
import {describe, expect, test, vi} from 'vitest'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import {atom} from '../src/atom.js'
import {autorun} from '../src/common.js'
import {computed, Computed, destroyComputed} from '../src/computed.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {batch, notifier} from '../src/notify.js'
import {createSelection, RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {RxTime} from '../src/RxTime.js'

/**
 * Executable evidence for confirmed issues.
 *
 * Each test started as a known-failing executable reproduction. Once its defect
 * was fixed, it became a normal regression test in this same file.
 */
describe('known RxList consistency issues', () => {
    test('findIndex follows reorder instead of throwing on ITERATE_KEY', () => {
        const source = new RxList([3, 1, 2])
        const index = source.findIndex(item => item === 1)
        try {
            expect(index()).toBe(1)
            expect(() => source.sortSelf((a, b) => a - b)).not.toThrow()
            expect(index()).toBe(0)
        } finally {
            destroyComputed(index)
            source.destroy()
        }
    })

    test('findIndex tracks reactive predicates introduced by set', () => {
        const source = new RxList([
            {score: atom(1)},
            {score: atom(3)},
        ])
        const index = source.findIndex(item => item.score() >= 3)
        const replacement = {score: atom(1)}
        try {
            source.set(0, replacement)
            expect(index()).toBe(1)

            replacement.score(4)
            expect(index()).toBe(0)
        } finally {
            destroyComputed(index)
            source.destroy()
        }
    })

    test('findIndex keeps reactive predicates after an unaffected structural patch', () => {
        const item = {score: atom(3)}
        const source = new RxList([item])
        const index = source.findIndex(value => value.score() >= 3)
        try {
            expect(index()).toBe(0)
            source.splice(1, 2)
            item.score(1)
            expect(index()).toBe(-1)
        } finally {
            destroyComputed(index)
            source.destroy()
        }
    })

    test('map rebuilds row dependencies after an explicit set', () => {
        const factor = atom(1)
        const source = new RxList([1, 2])
        const mapped = source.map(item => item * factor())
        try {
            source.set(0, 3)
            expect(mapped.data).toEqual([3, 2])

            factor(2)
            expect(mapped.data).toEqual([6, 4])
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('filter keeps source order for replacement splices', () => {
        const source = new RxList([0, 1, 2, 3, 4, 5])
        const filtered = source.filter(item => item % 3 === 0)
        try {
            source.splice(NaN as never, 1, 10, 11, 12)
            expect(filtered.data).toEqual(source.data.filter(item => item % 3 === 0))
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('filter follows source reorder operations', () => {
        const source = new RxList([3, 2, 1])
        const filtered = source.filter(item => item % 2 === 1)
        try {
            source.sortSelf((a, b) => a - b)
            expect(filtered.data).toEqual(source.data.filter(item => item % 2 === 1))
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('map keeps row effect frames aligned for a set after reorder', () => {
        const source = new RxList([13, 11, 5])
        const filtered = source.filter(item => item % 2 === 1)
        try {
            source.sortSelf((a, b) => a - b)
            source.set(0, 14)
            expect(filtered.data).toEqual(source.data.filter(item => item % 2 === 1))
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('slice with negative bounds matches native slice after middle insertion', () => {
        const source = new RxList([0, 1, 2, 3])
        const sliced = source.slice(-4, -1)
        try {
            source.splice(1, 0, 8, 9)
            expect(sliced.data).toEqual(source.data.slice(-4, -1))
        } finally {
            sliced.destroy()
            source.destroy()
        }
    })

    test('slice normalizes fractional bounds before applying incremental patches', () => {
        const source = new RxList([0, 1, 2, 3, 4])
        const sliced = source.slice(1.5, 4.8)
        try {
            source.splice(2, 1, 9)
            expect(sliced.data).toEqual(source.data.slice(1.5, 4.8))
        } finally {
            sliced.destroy()
            source.destroy()
        }
    })

    test('concat removes a duplicate from the source segment that changed', () => {
        const left = new RxList([1, 2])
        const right = new RxList([1, 3])
        const combined = left.concat(right)
        try {
            right.splice(0, 1)
            expect(combined.data).toEqual([...left.data, ...right.data])
        } finally {
            combined.destroy()
            left.destroy()
            right.destroy()
        }
    })

    test('toSet retains a value while another equal source item remains', () => {
        const source = new RxList([1, 1, 2])
        const set = source.toSet()
        try {
            source.splice(0, 1)
            expect([...set.data]).toEqual([...new Set(source.data)])
        } finally {
            set.destroy()
            source.destroy()
        }
    })

    test('groupBy incremental groups preserve full-recompute order', () => {
        const source = new RxList([1, 3])
        const groups = source.groupBy(item => item % 2)
        try {
            source.splice(1, 0, 5)
            expect(groups.data.get(1)?.data).toEqual(source.data.filter(item => item % 2 === 1))
        } finally {
            for (const group of groups.data.values()) group.destroy()
            groups.destroy()
            source.destroy()
        }
    })

    test('index-key selection applies RxSet.replace additions and deletions in the right direction', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = new RxSet([0])
        const selection = source.createIndexKeySelection(selected)
        try {
            selected.replace([1])
            expect(selection.data.map(([, indicator]) => indicator())).toEqual([false, true, false])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('reduceToAtom receives the real appended item index', () => {
        const source = new RxList(['a', 'b'])
        const indexSum = source.reduceToAtom((sum, _item, index) => sum + index, 0)
        try {
            source.push('c')
            expect(indexSum()).toBe(3)
        } finally {
            destroyComputed(indexSum)
            source.destroy()
        }
    })

    test('reduce appends effect frames instead of overwriting frame zero', () => {
        const source = new RxList([1, 2])
        const reduced = source.reduce<RxList<number>>((result, item) => {
            result.push(item)
            computed(() => item)
        }, RxList)
        try {
            expect(reduced.effectFramesArray.map(frame => frame.length)).toEqual([1, 1])
            source.push(3)
            expect(reduced.data).toEqual([1, 2, 3])
            expect(reduced.effectFramesArray.map(frame => frame.length)).toEqual([1, 1, 1])
        } finally {
            reduced.destroy()
            source.destroy()
        }
    })
})

describe('known exception-safety issues', () => {
    test('a computed that throws before its first dependency read remains subscribed for recovery', () => {
        const source = atom(0)
        let shouldThrow = false
        const value = computed(() => {
            if (shouldThrow) throw new Error('early failure')
            return source()
        })
        try {
            shouldThrow = true
            expect(() => source(1)).toThrow('early failure')

            shouldThrow = false
            source(2)
            expect(value()).toBe(2)
        } finally {
            destroyComputed(value)
        }
    })

    test('a throwing findIndex predicate restores all global tracking collectors', () => {
        const framesBefore = notifier.trackTargetFrames.slice()
        const currentFrameBefore = notifier.currentTrackFrame
        const stackBefore = notifier.trackStack.slice()
        const shouldTrackBefore = notifier.shouldTrack
        const source = new RxList([1])
        let laterComputed: ReturnType<typeof computed> | undefined

        try {
            expect(() => source.findIndex(() => {
                throw new Error('predicate failure')
            })).toThrow('predicate failure')

            const laterSource = atom(1)
            laterComputed = computed(() => laterSource())
            const leakedFrame = notifier.currentTrackFrame

            expect({
                frameDepth: notifier.trackTargetFrames.length,
                stackDepth: notifier.trackStack.length,
                shouldTrack: notifier.shouldTrack,
                retainedLaterTargets: leakedFrame?.length ?? 0,
            }).toEqual({
                frameDepth: framesBefore.length,
                stackDepth: stackBefore.length,
                shouldTrack: shouldTrackBefore,
                retainedLaterTargets: 0,
            })
        } finally {
            if (laterComputed) destroyComputed(laterComputed)
            source.destroy()
            notifier.trackTargetFrames.splice(0, notifier.trackTargetFrames.length, ...framesBefore)
            notifier.currentTrackFrame = currentFrameBefore
            notifier.trackStack.splice(0, notifier.trackStack.length, ...stackBefore)
            notifier.shouldTrack = shouldTrackBefore
        }
    })
})

describe('known AsyncRxSlice state issues', () => {
    test('successful full fetch clears isLoading', async () => {
        const slice = new AsyncRxSlice<number>([], async () => [1, 2])
        try {
            await slice.fetchFullRemoteData()
            expect(slice.data).toEqual([1, 2])
            expect(slice.isLoading()).toBe(false)
        } finally {
            slice.destroy()
        }
    })

    test('an older update response cannot overwrite a newer replacement', async () => {
        const resolvers = new Map<number, (items: number[]) => void>()
        const slice = new AsyncRxSlice<number>([], cursor => new Promise(resolve => {
            resolvers.set(cursor!, resolve)
        }))
        try {
            const older = slice.update(1, undefined, undefined, undefined, true)
            const newer = slice.update(2, undefined, undefined, undefined, true)

            resolvers.get(2)!([2])
            await newer
            expect(slice.data).toEqual([2])

            resolvers.get(1)!([1])
            await older
            expect(slice.data).toEqual([2])
        } finally {
            slice.destroy()
        }
    })

    test('a successful update clears a previous loadError', async () => {
        const slice = new AsyncRxSlice<number>([], async () => {
            throw new Error('old failure')
        })
        try {
            await slice.update(0)
            expect(slice.loadError()).toBeInstanceOf(Error)

            slice.getRemoteData = async () => [9]
            await slice.update(0, undefined, undefined, undefined, true)
            expect(slice.data).toEqual([9])
            expect(slice.loadError()).toBeNull()
        } finally {
            slice.destroy()
        }
    })
})

describe('known trigger-payload ownership issues', () => {
    // 2026-H3 round5 动态复现,裁定已执行(防御拷贝):splice/clear 等的返回数组
    // 曾与 info.methodResult 同引用,batch/async applyPatch/onChange/自定义调度器
    // 四类延迟消费窗口里调用方按原生 splice 预期改写返回数组会静默毒化全部
    // patch 消费者与 digestReplay 重建。现协议载荷持独立副本(util.toProtocolPayload,
    // dev 下冻结广播载荷),返回数组归调用方所有。等价类横扫见
    // deepReview2026H3Round5.spec.ts 的 R5-D1 组。
    test('splice return array can be mutated inside batch without corrupting deferred patches', () => {
        const list = new RxList<number>([1, 2, 3, 4])
        const groups = list.groupBy(item => item % 2)
        try {
            batch(() => {
                const removed = list.splice(0, 2)
                removed.length = 0 // 调用方按原生 splice 语义"回收"返回数组
            })
            expect({
                g0: groups.data.get(0) ? [...groups.data.get(0)!.data] : null,
                g1: groups.data.get(1) ? [...groups.data.get(1)!.data] : null,
            }).toEqual({g0: [4], g1: [3]})
        } finally {
            for (const group of groups.data.values()) group.destroy()
            groups.destroy()
            list.destroy()
        }
    })
})

describe('known selection equality-semantics issues (2026-H3 round6, fixed)', () => {
    // 2026-H3 round6 动态复现,已修复:createSelection 的 atom 单选模式曾在三个
    // 入口用 `===` 判等,而记账 Map(itemToIndicators)与 RxSet 多选路径都是
    // SameValueZero——NaN item 下三个入口互相分叉:
    //   1) createNewIndicator: `currentValues.raw === item` → 全量重建时 NaN 行恒 false;
    //   2) updateIndicatorsFromCurrentValueChange(atom 分支) → Map.get(NaN) 命中 → 增量置 true;
    //   3) deleteCurrentValueIfItemRemoved: `item === currentValues.raw` → NaN 选中值
    //      在 item 删除后不回收(RxSet 分支的 Set.has 可回收,两模式行为不一致)。
    // 修复:atom 分支统一 SameValueZero(与记账/RxSet 侧对齐,判等门覆盖同一
    // 语义的全部入口)。等价类横扫见 deepReview2026H3Round6.spec.ts 的 R6-1 组。
    test('atom-mode selection keeps NaN indicators consistent between incremental update and full rebuild', () => {
        const source = new RxList<number>([NaN, 1])
        const selected = atom<number | null>(null)
        const incrementalSel = createSelection(source, selected as any)
        let fullSel: ReturnType<typeof createSelection<number>> | undefined
        try {
            selected(NaN)
            const incremental = incrementalSel.data.map(([, indicator]) => indicator.raw)
            // 同一 source/currentValues 的全新构建 = 全量重算语义
            fullSel = createSelection(source, selected as any)
            const full = fullSel.data.map(([, indicator]) => indicator.raw)
            expect(incremental).toEqual(full)
        } finally {
            fullSel?.destroy()
            incrementalSel.destroy()
            source.destroy()
        }
    })

    test('atom-mode selection with autoResetValue releases a removed NaN item like the RxSet mode does', () => {
        const source = new RxList<number>([NaN, 1, 2])
        const selected = atom<number | null>(NaN)
        const selection = createSelection(source, selected as any, true)
        try {
            source.splice(0, 1) // 删除唯一的 NaN 行
            // RxSet 多选模式(SameValueZero)会回收选中值;atom 模式应一致
            expect(selected.raw).toBeNull()
        } finally {
            selection.destroy()
            source.destroy()
        }
    })
})

describe('known lazily-cached-structure lifecycle issues (2026-H3 round6, fixed)', () => {
    // 2026-H3 round6 动态复现,已修复:AsyncRxSlice.fetch() 惰性创建的
    // autoFetchPromise computed 曾没有用 ReactiveEffect.createDetached 隔离
    // ——首次 fetch 发生在 autorun/computed 内(条件驱动拉取是常见形态)时被收集
    // 为宿主 child,宿主重算的 destroyChildren 把它销毁;autoFetchPromise 字段仍
    // 指向已销毁实例,此后 getRemoteData 的响应式参数变化不再触发重新拉取,
    // fetch() 永远返回旧 promise(静默陈旧,无任何报警)。
    // 与历史缺陷「RxList.length 在 autorun 中读会被当作 children 误销毁」同一
    // 等价类(实例缓存的惰性结构 × 创建作用域生命周期)。修复:autoFetchPromise
    // 与 RxTime.resolve 的内部 autorun(同类漏网入口)都改经 createDetached 创建;
    // 全等价类横扫(length/keys/values/entries/size 家族)见
    // deepReview2026H3Round6.spec.ts 的 R6-2 组。
    test('AsyncRxSlice keeps auto-refetching after the reactive scope that first called fetch() reruns', async () => {
        const fetchedPages: number[] = []
        const page = atom(1)
        const slice = new AsyncRxSlice<number>([], async () => {
            const currentPage = page() // 第一个 await 之前的同步读:建立依赖
            fetchedPages.push(currentPage)
            return [currentPage]
        })
        const rerun = atom(0)
        const stop = autorun(() => {
            rerun()
            slice.fetch()
        }, true)
        try {
            await new Promise(resolve => setTimeout(resolve, 5))
            expect(fetchedPages).toEqual([1])
            rerun(1) // 宿主重算 → destroyChildren 销毁 autoFetchPromise
            await new Promise(resolve => setTimeout(resolve, 5))
            page(2) // 拉取参数变化,应重新拉取
            await new Promise(resolve => setTimeout(resolve, 5))
            expect(fetchedPages).toContain(2)
        } finally {
            stop()
            slice.destroy()
        }
    })
})

describe('known batch error-masking issues (2026-H3 round6, fixed)', () => {
    // 2026-H3 round6 动态复现,已修复(裁定 = body 异常优先):batch() 的 fn 抛错
    // 后,finally 中 digestEffectSession 若也重抛订阅者的 firstError,JS 的
    // finally-throw 语义会**静默替换**在途的 body 异常——调用方只看到订阅者
    // 错误,自己代码的原始异常完全丢失(也没有 console.error 兜底)。
    // 修复:body 异常在途时 digest 照常执行(其余订阅者不受牵连),订阅者错误
    // 降级 console.error 上报,body 异常照常传播;无 body 异常时行为不变
    // (第一个订阅者错误抛给调用方,README §3 原契约)。错误组合矩阵见
    // deepReview2026H3Round6.spec.ts 的 R6-3 组。
    test('batch propagates the body error instead of silently replacing it with a subscriber error', () => {
        const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
        const source = atom(1)
        const stop = autorun(() => {
            if (source() > 1) throw new Error('SUBSCRIBER_ERROR')
        }, true)
        let caught: unknown
        try {
            batch(() => {
                source(2) // 排队一个 digest 时会抛错的订阅者
                throw new Error('BODY_ERROR')
            })
        } catch (error) {
            caught = error
        } finally {
            stop()
            consoleSpy.mockRestore()
        }
        expect((caught as Error).message).toBe('BODY_ERROR')
    })
})

describe('known atom.fixed/atom.lazy interface issues (2026-H3 round6, fixed)', () => {
    // 2026-H3 round6 动态复现,已修复(裁定 = 补全接口):atom.fixed/atom.lazy
    // 通过 IS_ATOM 标记宣称自己是 Atom(isAtom 为 true),却曾不实现 AtomBase 的
    // `.raw`——所有「isAtom 检查后读 .raw」的消费点拿到 undefined,最直接的是
    // AtomComputed.replaceData 的解包路径:computed(() => atom.fixed(42)) 的值
    // 是 undefined 而不是 42(atom.lazy 同形)。修复:fixed/lazy 补 raw 访问器
    // (语义与两种 atom 形态一致:读值不追踪,lazy 经 pauseTracking 隔离)。
    // 接口契约横扫见 deepReview2026H3Round6.spec.ts 的 R6-4 组。
    test('computed getter returning atom.fixed resolves to the fixed value', () => {
        const fixed = atom.fixed(42)
        const result = computed(() => fixed)
        try {
            expect(result.raw).toBe(42)
        } finally {
            destroyComputed(result)
        }
    })
})

describe('known RxTime semantic issues', () => {
    // 已知未修（2026-H3 round4 review 动态复现，待语义裁定）：eq 的唤醒 timeout
    // 刻意排在越点后 +2ms（浮点防御），而谓词是点相等 v === 0——唤醒时刻已越过
    // 零点，eq 永远观察不到 true。定时器的存在本身说明"越点应可观察"是实现
    // 意图（否则无须排定唤醒）；gt/lt 不受影响（不等式越点后稳定成立）。
    // 裁定方向：脉冲语义 / 容差窗口 / 移除 eq。修复时按 AGENTS 纪律把本测试
    // 转为普通回归（test.fails 会在行为变化时主动报警）。
    test.fails('RxTime.eq(future instant) becomes observable after the crossing', () => {
        vi.useFakeTimers()
        try {
            vi.setSystemTime(1_000_000)
            const t = new RxTime()
            const isEq = t.eq(1_000_500)
            let sawTrue = isEq.raw === true
            const stop = autorun(() => {
                if (isEq()) sawTrue = true
            }, true)
            vi.advanceTimersByTime(600)
            stop()
            t.destroy()
            expect(sawTrue).toBe(true)
        } finally {
            vi.useRealTimers()
        }
    })
})

describe('known inline-dispatch channel issues (2026-H3 round9, pending adjudication)', () => {
    // 方法 25(派发通道敌意差分)动态复现,待裁定。攻击轴:同一「多订阅者 ×
    // 敌意场景(错误注入/派发中重入写)」在三条派发通道下执行——
    //   ① 非 batch atom 写的内联通道(triggerPrimitiveAtomValue/triggerEffects 循环);
    //   ② batch 的 session digest 通道;
    //   ③ Rx 结构变更方法的结构通道(dispatchStructuralThen/sendTriggerInfos,恒有 session)。
    // ②③ 对两个敌意场景都有防线(逐 effect try/catch;重入写 info 追加到队尾保持
    // 因果序),① 双双缺失——同一语义三个入口保护深度不一致(R7 兄弟实现点纪律、
    // R8-8「一 loud 一 silent」的同族形态)。

    // 缺陷形态 1:内联通道首订阅者抛错 → 剩余订阅者既不执行也不标脏(status 停留
    // CLEAN),静默陈旧;幂等重写(Object.is 判等门拦截)无法救回,只有写入**不同值**
    // 或 force recompute 才能追平。batch 通道同场景下兄弟订阅者照常执行
    // (digest 逐 effect try/catch,首错 digest 后重抛),README §3 只对 batch 成文。
    // 裁定方向:内联循环补 digest 同款隔离(首错循环后重抛) vs README 把
    // 「非 batch 首订阅者抛错跳过其余订阅者」成文为契约边界。
    test.fails('inline atom dispatch: sibling subscribers still run when the first subscriber throws', () => {
        const a = atom(0)
        let boomOnce = true
        const c1 = computed(() => {
            const v = a() as number
            if (v === 1 && boomOnce) { boomOnce = false; throw new Error('subscriber boom') }
            return v
        })
        const c2 = computed(() => (a() as number) * 2)
        try {
            expect(() => a(1)).toThrow('subscriber boom')
            // batch 通道同场景下 c2 会是 2(deepReview round6 R6-3 组);内联通道
            // 目前是 0(静默陈旧),且 a(1) 幂等重写被判等门拦截、无法救回。
            expect(c2.raw).toBe(2)
        } finally {
            destroyComputed(c1)
            destroyComputed(c2)
        }
    })

    // 缺陷形态 2:派发中重入写(handleTriggered 注释点名支持的「平衡状态」回写:
    // 先订阅的 autorun 看到非法值同步改写同一 atom)下,内联通道对**后订阅**的
    // 消费者交付乱序 info——嵌套写的 [1→2] 先于外层写的 [null→1] 到达(onChange
    // 实测 [[1,2],[null,1]]),delta 型消费者(createSelection/createIndexKeySelection
    // 的 atom 单选分支按 oldValue→false/newValue→true 增量消费)据此把 indicator
    // 置成「1 与 2 同时选中」;后续正常写入永远不再触及 item 1 → **永久卡 true**,
    // 只有 force recompute 能追平(A1 边界「终值错误/不收敛」,属"仍属缺陷"域)。
    // 同场景 batch 通道 info 保持因果序、终态正确;RxSet 多选模式(结构通道,
    // 恒有 session)同样正确——同一 API 的两个模式(R6-1 轴)× 两条通道分叉。
    // 裁定方向:(a) 内联 atom 派发包一层 micro-session(与结构通道对齐,热路径
    // 代价待测);(b) selection 家族的 atom 分支改「按终态 raw 对账」而不是信任
    // delta(消费侧修复,不救 onChange 用户);(c) README 把「内联重入写下 info
    // 对后订阅者非因果序」成文为契约边界并给出 batch 规避指引。
    test.fails('inline reentrant atom write: selection indicators converge to the full-recompute state', () => {
        const list = new RxList<number>([0, 1, 2])
        const cur = atom<number | null>(null)
        // 先订阅的平衡回写:1 不可选,自动跳到 2(与 handleTriggered 注释的
        // pending→processing 模式同形)
        const stop = autorun(() => {
            if (cur() === 1) cur(2)
        }, true)
        const selection = createSelection(list, cur)
        try {
            cur(1)
            expect(cur.raw).toBe(2)
            // 全量重算语义:只有 item 2 选中。当前实测 [false, true, true],
            // 且 cur(0)/cur(null) 等后续写入永远修不掉 item 1 的 true。
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, false, true])
        } finally {
            stop()
            selection.destroy()
            list.destroy()
        }
    })
})

describe('known skipIndicator semantic gaps (2026-H3 round9, pending adjudication)', () => {
    // 静态确认 + 动态复现,待裁定。skipIndicator 是 computed() 的公开第 5 参
    // (axii 自己的 LightBindingEffect 只在本地消费同名概念,并不传给 data0),
    // 在 data0 仓内:零 README 文档、零测试、coreSurfaceInventory 零登记
    // (账本按导出面普查,参数级表面漏网)。
    // 动态行为:Computed.run/runFromTrigger/runFromAtomTrigger 在 skip 窗口内
    // 直接 return——info 被丢弃且不标脏。对 patch 型 computed,解除 skip 后的
    // 下一次触发只增量重放**新** info,skip 窗口内丢弃的变更永久缺失(增量 ≠
    // 全量重算,静默分叉,只有 force recompute 能追平)。非 patch 型解除后一次
    // 触发即自愈(全量读终态)。
    // 裁定方向:skip 窗口内至少记「错过触发」标记,解除后首次触发回退全量
    // (与 handleRecomputeError 的 FULL_RECOMPUTE_PHASE 复位同款);或文档成文
    // 「skip 期间的变更永久丢失,解除后须自行 force recompute」。
    test.fails('patch computed catches up with mutations dropped during a skip window', () => {
        const source = new RxList<number>([1, 2, 3])
        const skip = {skip: false}
        const mirror = computed<number[]>(
            function computation(this: Computed) {
                ;(this as any).manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            function applyPatch(this: Computed, data: any, infos: any[]) {
                const arr = (data.raw as number[]).slice()
                for (const info of infos) {
                    if (info.method !== 'splice') return false
                    arr.splice(info.argv[0] as number, (info.methodResult as unknown[]).length, ...info.argv.slice(2))
                }
                data(arr)
            },
            true,
            undefined,
            skip,
        )
        try {
            expect(mirror.raw).toEqual([1, 2, 3])
            skip.skip = true
            source.push(4) // skip 窗口内的变更:info 被 run() 丢弃
            skip.skip = false
            source.push(5) // 解除后的变更:patch 只重放本条
            // 全量重算语义应为 [1,2,3,4,5];当前实测 [1,2,3,5](4 永久缺失)
            expect(mirror.raw).toEqual([1, 2, 3, 4, 5])
        } finally {
            destroyComputed(mirror)
            source.destroy()
        }
    })
})

describe('repository and release evidence', () => {
    test('package manifest and pnpm importer use identical dependency specifiers', () => {
        const packageJson = JSON.parse(readFileSync(resolve('package.json'), 'utf8')) as {
            devDependencies: Record<string, string>
        }
        const lockfile = readFileSync(resolve('pnpm-lock.yaml'), 'utf8')
        const importerStart = lockfile.indexOf('importers:')
        const packagesStart = lockfile.indexOf('\npackages:')
        const importer = lockfile.slice(importerStart, packagesStart)
        const lockSpecifiers = new Map<string, string>()
        let currentDependency: string | undefined
        for (const line of importer.split('\n')) {
            const dependency = line.match(/^ {6}(.+):$/)
            if (dependency) {
                currentDependency = dependency[1].replace(/^['"]|['"]$/g, '')
                continue
            }
            const specifier = line.match(/^ {8}specifier:\s+(.+)$/)
            if (currentDependency && specifier) {
                lockSpecifiers.set(
                    currentDependency,
                    specifier[1].trim().replace(/^['"]|['"]$/g, ''),
                )
            }
        }

        const mismatches = Object.entries(packageJson.devDependencies).flatMap(([name, expected]) => {
            const actual = lockSpecifiers.get(name) ?? '<missing>'
            return actual === expected ? [] : [{name, expected, actual}]
        })

        expect(mismatches).toEqual([])
    })

    test('package manifest wires dual dev/prod builds and require-condition types', () => {
        // 2026-H3 round6 工程面契约:dev 构建经 `development` 条件触达消费者的
        // 开发模式(纯度探测/销毁警告/不变量断言),无条件解析仍拿 prod;
        // require 条件必须有 .d.cts(node16 解析下 ESM 形状的 d.ts 会被判伪装)。
        const packageJson = JSON.parse(readFileSync(resolve('package.json'), 'utf8'))
        const root = packageJson.exports['.']
        expect(root.import.development).toBe('./dist/data0.dev.js')
        expect(root.import.default).toBe('./dist/data0.js')
        expect(root.require.development).toBe('./dist/data0.dev.umd.cjs')
        expect(root.require.default).toBe('./dist/data0.umd.cjs')
        expect(root.import.types).toBe('./dist/index.d.ts')
        expect(root.require.types).toBe('./dist/index.d.cts')
        // 旧字段对齐:main 供无 exports 支持的 CJS 解析(必须是 cjs 而不是 ESM),
        // module 供老打包器;build 脚本必须同时产出双构建与 .d.cts。
        expect(packageJson.main).toBe('dist/data0.umd.cjs')
        expect(packageJson.module).toBe('dist/data0.js')
        expect(packageJson.scripts.build).toContain('--mode dev')
        expect(packageJson.scripts.build).toContain('postbuild')
    })

    if (process.platform === 'win32') {
        test.skip('release pushes version tags to the remote', () => {})
    } else {
        test('release pushes version tags to the remote', () => {
            // 2026-H3 round6 工程面静态确认的缺陷:裸 `git push` 不推 tags,
            // v2.10.0-v2.12.0 三个已发布版本远端无对应 tag。现在 release 脚本
            // 必须 --follow-tags(annotated)+ --tags(lightweight 兜底)。
            const sandbox = mkdtempSync(join(tmpdir(), 'data0-release-tags-'))
            const fakeBin = join(sandbox, 'bin')
            const gitLog = join(sandbox, 'git-args.log')
            mkdirSync(fakeBin)
            // fake git 把参数记录进日志(stdout 保持干净,git status 的 isClean 判定依赖空输出)
            writeFileSync(join(fakeBin, 'git'), `#!/bin/sh\necho "$@" >> ${JSON.stringify(gitLog)}\nexit 0\n`)
            chmodSync(join(fakeBin, 'git'), 0o755)
            writeFileSync(join(fakeBin, 'pnpm'), '#!/bin/sh\nexit 0\n')
            chmodSync(join(fakeBin, 'pnpm'), 0o755)
            try {
                const result = spawnSync(process.execPath, [resolve('scripts/release.js'), '9.9.9'], {
                    cwd: sandbox,
                    env: {...process.env, PATH: `${fakeBin}:${process.env.PATH ?? ''}`},
                    encoding: 'utf8',
                    timeout: 5_000,
                })
                expect(result.status).toBe(0)
                const calls = readFileSync(gitLog, 'utf8')
                expect(calls).toContain('push --follow-tags')
                expect(calls).toContain('push --tags')
            } finally {
                rmSync(sandbox, {recursive: true, force: true})
            }
        })

        test('release version argument cannot execute a second shell command', () => {
            const sandbox = mkdtempSync(join(tmpdir(), 'data0-release-repro-'))
            const fakeBin = join(sandbox, 'bin')
            const marker = join(sandbox, 'injected')
            mkdirSync(fakeBin)

            const writeFakeCommand = (name: string) => {
                const commandPath = join(fakeBin, name)
                writeFileSync(commandPath, '#!/bin/sh\nexit 0\n')
                chmodSync(commandPath, 0o755)
            }
            writeFakeCommand('git')
            writeFakeCommand('npm')
            writeFakeCommand('pnpm')

            try {
                const payload = `1.2.3; touch ${JSON.stringify(marker)}`
                const result = spawnSync(process.execPath, [resolve('scripts/release.js'), payload], {
                    cwd: sandbox,
                    env: {
                        ...process.env,
                        PATH: `${fakeBin}:${process.env.PATH ?? ''}`,
                    },
                    encoding: 'utf8',
                    timeout: 5_000,
                })

                expect(result.status).toBe(0)
                expect(existsSync(marker)).toBe(false)
            } finally {
                rmSync(sandbox, {recursive: true, force: true})
            }
        })
    }
})
