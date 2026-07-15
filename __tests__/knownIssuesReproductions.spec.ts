import {chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs'
import {tmpdir} from 'node:os'
import {join, resolve} from 'node:path'
import {spawnSync} from 'node:child_process'
import {describe, expect, test, vi} from 'vitest'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import {atom} from '../src/atom.js'
import {autorun} from '../src/common.js'
import {computed, destroyComputed} from '../src/computed.js'
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

describe('known selection equality-semantics issues (2026-H3 round6)', () => {
    // 已知未修(2026-H3 round6 review 动态复现,待修复):createSelection 的 atom
    // 单选模式在三个入口用 `===` 判等,而记账 Map(itemToIndicators)与 RxSet
    // 多选路径都是 SameValueZero——NaN item 下三个入口互相分叉:
    //   1) createNewIndicator: `currentValues.raw === item` → 全量重建时 NaN 行恒 false;
    //   2) updateIndicatorsFromCurrentValueChange(atom 分支) → Map.get(NaN) 命中 → 增量置 true;
    //   3) deleteCurrentValueIfItemRemoved: `item === currentValues.raw` → NaN 选中值
    //      在 item 删除后不回收(RxSet 分支的 Set.has 可回收,两模式行为不一致)。
    // 违反「增量 ≡ 全量重算」不变量与方法 18/19 的「判等门覆盖同一语义所有入口」
    // 规则。修复方向:atom 分支统一 SameValueZero(与记账/RxSet 侧对齐)。
    // 备注:coverageInventory 的 createSelection × weirdNum 格子引用的资产只覆盖
    // RxSet 多选路径,atom 单选路径是「值域 × 入口」盲格(§3.3 R4-2 同款教训)。
    test.fails('atom-mode selection keeps NaN indicators consistent between incremental update and full rebuild', () => {
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

    test.fails('atom-mode selection with autoResetValue releases a removed NaN item like the RxSet mode does', () => {
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

describe('known lazily-cached-structure lifecycle issues (2026-H3 round6)', () => {
    // 已知未修(2026-H3 round6 review 动态复现,待修复):AsyncRxSlice.fetch() 惰性
    // 创建的 autoFetchPromise computed 没有用 ReactiveEffect.createDetached 隔离
    // ——首次 fetch 发生在 autorun/computed 内(条件驱动拉取是常见形态)时被收集
    // 为宿主 child,宿主重算的 destroyChildren 把它销毁;autoFetchPromise 字段仍
    // 指向已销毁实例,此后 getRemoteData 的响应式参数变化不再触发重新拉取,
    // fetch() 永远返回旧 promise(静默陈旧,无任何报警)。
    // 与历史缺陷「RxList.length 在 autorun 中读会被当作 children 误销毁」同一
    // 等价类(实例缓存的惰性结构 × 创建作用域生命周期);RxList.length/
    // RxMap.keys/size 已用 createDetached 修复,AsyncRxSlice.autoFetchPromise
    // 是该等价类的漏网入口(RxTime.resolve 的 autorun 同属此类,未单独钉扎)。
    test.fails('AsyncRxSlice keeps auto-refetching after the reactive scope that first called fetch() reruns', async () => {
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

describe('known batch error-masking issues (2026-H3 round6)', () => {
    // 已知未修(2026-H3 round6 review 动态复现,待裁定):batch() 的 fn 抛错后,
    // finally 中 digestEffectSession 若也重抛订阅者的 firstError,JS 的
    // finally-throw 语义会**静默替换**在途的 body 异常——调用方只看到订阅者
    // 错误,自己代码的原始异常完全丢失(也没有 console.error 兜底)。
    // README §3 只裁定了「订阅者互不阻断,第一个错误抛给调用方」,没有裁定
    // body 异常与订阅者异常同时出现时的优先级。裁定方向:body 异常优先传播、
    // 订阅者错误 console.error 上报(或两者聚合成 AggregateError)。
    test.fails('batch propagates the body error instead of silently replacing it with a subscriber error', () => {
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
        }
        expect((caught as Error).message).toBe('BODY_ERROR')
    })
})

describe('known atom.fixed/atom.lazy interface issues (2026-H3 round6)', () => {
    // 已知未修(2026-H3 round6 review 动态复现,待裁定):atom.fixed/atom.lazy
    // 通过 IS_ATOM 标记宣称自己是 Atom(isAtom 为 true),却不实现 AtomBase 的
    // `.raw`——所有「isAtom 检查后读 .raw」的消费点拿到 undefined。
    // 最直接的消费点是 AtomComputed.replaceData(computed getter 返回 atom 时
    // 解包 .raw 存快照):computed(() => atom.fixed(42)) 的值是 undefined 而不是
    // 42(atom.lazy 同形)。裁定方向:给 fixed/lazy 补 raw 访问器,或让它们
    // 不再通过 isAtom,或把「computed 返回 atom 会解包 raw 快照」的行为整体成文。
    test.fails('computed getter returning atom.fixed resolves to the fixed value', () => {
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

    if (process.platform === 'win32') {
        test.skip('release version argument cannot execute a second shell command', () => {})
    } else {
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
