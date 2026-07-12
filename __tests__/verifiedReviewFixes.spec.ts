/**
 * 等价类回归：2026-07 深度评估中已动态复现的缺陷修复。
 *
 * 1. 越界 set × map(index) → TypeError，并毒化后续契约内 push
 * 2. object atom 经 proxy 的浅属性写入必须通知订阅者
 * 3. setupVitestEnv 不得残留 debugger（由 reviewFixes / 卫生检查覆盖路径）
 */
import {describe, expect, test} from 'vitest'
import {atom, autorun, computed, Computed, destroyComputed, ReactiveEffect, RxList} from '../src'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations'
import {readFileSync} from 'node:fs'
import {resolve} from 'node:path'

describe('F1: 越界 set × map(index) 不再崩溃', () => {
    test('set(OOB) after map with index updates mapped row and keeps later push working', () => {
        const list = new RxList([1, 2, 3])
        const mapped = list.map((item, index) => `${index()}:${item}`)
        expect(mapped.data).toEqual(['0:1', '1:2', '2:3'])

        expect(() => list.set(5, 99)).not.toThrow()
        expect(list.data.length).toBe(6)
        expect(list.data[5]).toBe(99)
        expect(mapped.data[5]).toBe('5:99')
        expect(5 in mapped.data).toBe(true)
        // 稀疏洞：与 Array 赋值语义一致，洞位不在 mapped 上物化为显式行键之外的行为
        expect(3 in list.data).toBe(false)
        expect(4 in list.data).toBe(false)

        expect(() => list.push(7)).not.toThrow()
        expect(mapped.data[6]).toBe('6:7')
        expect(list.atomIndexes?.[5]?.raw).toBe(5)
        expect(list.atomIndexes?.[6]?.raw).toBe(6)

        list.destroy()
        mapped.destroy()
    })

    test('set(OOB) without index-map still creates sparse mapped rows', () => {
        const list = new RxList([1, 2, 3])
        const mapped = list.map((item) => item * 2)
        list.set(5, 99)
        expect([...mapped.data]).toEqual([2, 4, 6, undefined, undefined, 198])
        list.push(7)
        expect(mapped.data[6]).toBe(14)
        list.destroy()
        mapped.destroy()
    })

    test('in-contract set with index-map unchanged', () => {
        const list = new RxList([1, 2, 3])
        const mapped = list.map((item, index) => `${index()}:${item}`)
        list.set(1, 20)
        expect(mapped.data).toEqual(['0:1', '1:20', '2:3'])
        list.destroy()
        mapped.destroy()
    })
})

describe('F2: object atom 浅属性写入触发通知', () => {
    test('proxy property set notifies sync autorun', () => {
        const obj = atom<{count: number; nested: {n: number}}>({count: 1, nested: {n: 1}})
        let runs = 0
        let lastCount = 0
        const stop = autorun(() => {
            runs++
            lastCount = obj.count
        }, true)

        expect(runs).toBe(1)
        obj.count = 2
        expect(runs).toBe(2)
        expect(lastCount).toBe(2)
        expect(obj.raw.count).toBe(2)

        // 深路径直写仍不触发（无深 Proxy）
        obj.raw.nested.n = 9
        expect(runs).toBe(2)
        expect(obj.raw.nested.n).toBe(9)

        obj({count: 3, nested: {n: 1}})
        expect(runs).toBe(3)
        expect(lastCount).toBe(3)
        stop()
    })

    test('identical property write does not re-notify', () => {
        const obj = atom({x: 1})
        let runs = 0
        const stop = autorun(() => {
            runs++
            void obj.x
        }, true)
        expect(runs).toBe(1)
        obj.x = 1
        expect(runs).toBe(1)
        stop()
    })
})

describe('F4: destroy-inside-run（下游错误边界的同步重入销毁）', () => {
    test('destroyComputed inside own sync applyPatch defers, then fully destroys', () => {
        const list = new RxList<string>(['a'])
        let patchRan = 0
        let insidePatchError: unknown = null
        // 与 axle RxListHost 相同的工厂形态
        const c = computed(
            function computation(this: Computed) {
                this.manualTrack(list, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return list.data.slice()
            },
            function applyPatch(this: Computed) {
                patchRan++
                // axle 模式：行错误 → error 钩子 → root.destroy() → destroyComputed(正在 patch 的自己)
                try {
                    destroyComputed(c)
                } catch (e) {
                    insidePatchError = e
                }
            }
        )

        expect(() => list.push('x')).not.toThrow()
        expect(insidePatchError).toBeNull()
        expect(patchRan).toBe(1)
        // 栈展开后销毁已生效：不再接收更新
        expect(() => list.push('y')).not.toThrow()
        expect(patchRan).toBe(1)
        // 全局栈无泄漏
        expect(ReactiveEffect.activeScopes.length).toBe(0)
        list.destroy()
    })

    test('destroy inside own getter (initial run) defers and does not corrupt sibling tracking', () => {
        const dep = atom(0)
        let selfDestroyed!: Computed
        const c = new Computed(function (this: Computed) {
            const v = dep()
            if (v === 0) {
                selfDestroyed = this
                this.destroy()
            }
            return v
        })
        expect(() => c.run([], true)).not.toThrow()
        expect(selfDestroyed.active).toBe(false)

        // 同一 dep 的其他订阅者不受 marker 污染：正常建立订阅并接收更新
        let seen = -1
        const stop = autorun(() => { seen = dep() }, true)
        dep(5)
        expect(seen).toBe(5)
        stop()
        expect(ReactiveEffect.activeScopes.length).toBe(0)
    })

    test('destroy() of a running autorun from inside itself is safe and final', () => {
        const a = atom(0)
        let runs = 0
        let stop!: () => void
        stop = autorun(() => {
            runs++
            a()
            if (runs === 2) stop()
        }, true)
        a(1) // triggers second run which self-stops
        const runsAfterStop = runs
        a(2)
        expect(runs).toBe(runsAfterStop)
        expect(ReactiveEffect.activeScopes.length).toBe(0)
    })
})

describe('F3: setupVitestEnv 无 debugger', () => {
    test('setupVitestEnv.ts has no debugger statement', () => {
        const src = readFileSync(resolve(__dirname, '../setupVitestEnv.ts'), 'utf8')
        expect(src).not.toMatch(/\bdebugger\b/)
    })
})
