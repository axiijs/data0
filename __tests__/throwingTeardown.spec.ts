/**
 * 父 effect 重算时,上一轮子 effect 的清理抛错:prepareTracking 要么完成,要么不留痕迹。
 *
 * prepareTracking 先压栈、再销毁上一轮的子 effect;销毁会执行用户清理。调用点(callSimpleGetter/
 * callAsyncGetter)把它放在 try 之外,基类 run() 又把它放在 try 之内却无条件 resetTracking:
 *   - Computed 宿主永久留在 activeScopes 上,之后任何顶层读取都订阅它(全局污染);
 *   - 裸 effect 宿主丢掉全部依赖,从此不再响应;嵌套时多弹一层 trackStack,dev 断言盖住原始错误;
 *   - async getter 卡在 RECOMPUTING,asyncStatus 恒 true,cleanPromise 永不结束(generator 是对的)。
 *
 * 等价类:{宿主形态} × {顶层/嵌套在别的 effect 里}。每一格都断言:错误按该宿主的通道送达、
 * 全局状态复原、受害者(兄弟订阅者、外层 effect、无关读取)不受影响、恢复后照常工作。
 */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest'
import {
    atom,
    Atom,
    AtomComputed,
    autorun,
    batch,
    Computed,
    computed,
    destroyComputed,
    Notifier,
    notifier,
    ReactiveEffect,
    RxList,
    RxSet,
    STATUS_CLEAN,
    STATUS_DIRTY,
    TrackOpTypes,
} from '../src/index.js'

const subscribers = (target: object) => [...(notifier.getDepEffects(target) ?? [])]

const trackingState = () => ({
    scopes: ReactiveEffect.activeScopes.length,
    depth: notifier.effectTrackDepth,
    bit: Notifier.trackOpBit,
    trackStack: notifier.trackStack.length,
    shouldTrack: notifier.shouldTrack,
})

async function settle() {
    for (let i = 0; i < 50; i++) await Promise.resolve()
}

let errorLog: unknown[][]
beforeEach(() => {
    errorLog = []
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    vi.spyOn(console, 'error').mockImplementation((...args) => { errorLog.push(args) })
})

afterEach(() => {
    vi.restoreAllMocks()
    const state = trackingState()
    // 先复位再断言,一个失败的用例不拖垮后面的
    ReactiveEffect.activeScopes.length = 0
    notifier.effectTrackDepth = 0
    Notifier.trackOpBit = 1
    notifier.trackStack.length = 0
    notifier.shouldTrack = true
    expect(state).toEqual({scopes: 0, depth: 0, bit: 1, trackStack: 0, shouldTrack: true})
})

// 与 axii 的轻量绑定同形:直接继承 ReactiveEffect
class LightEffect extends ReactiveEffect {
    constructor(fn: () => any) {
        super(fn)
        this.run()
    }
    callGetter() {
        return this.getter!()
    }
    run() {
        if (!this.active) return
        return super.run()
    }
}

// delivery:错误送达的通道。sync = 抛给写入方;console = 没有同步调用方,console.error 上报
type Host = {destroy: () => void, effect?: () => Computed}
type HostCase = {delivery: 'sync' | 'console', create: (dep: Atom<number>, body: () => void) => Host}

const HOSTS: Record<string, HostCase> = {
    'autorun': {delivery: 'sync', create: (dep, body) => ({destroy: autorun(() => { dep(); body() }, true)})},
    'autorun, default scheduling': {delivery: 'console', create: (dep, body) => ({destroy: autorun(() => { dep(); body() })})},
    'computed': {
        delivery: 'sync', create: (dep, body) => {
            const c = computed(() => { body(); return dep() })
            return {destroy: () => destroyComputed(c)}
        },
    },
    'light effect': {
        delivery: 'sync', create: (dep, body) => {
            const effect = new LightEffect(() => { dep(); body() })
            return {destroy: () => effect.destroy()}
        },
    },
    'async getter computed': {
        delivery: 'console', create: (dep, body) => {
            const effect = new AtomComputed(async () => { dep(); body() }, undefined, true)
            return {destroy: () => effect.destroy(), effect: () => effect}
        },
    },
    'generator getter computed': {
        delivery: 'console', create: (dep, body) => {
            const effect = new AtomComputed(function* (): any { dep(); body() }, undefined, true)
            return {destroy: () => effect.destroy(), effect: () => effect}
        },
    },
    'patch computed falling back to a full recompute': {
        delivery: 'sync', create: (dep, body) => {
            const c = computed<number>(function (this: Computed) {
                this.manualTrack(dep, TrackOpTypes.ATOM, 'value')
                body()
                return dep.raw
            }, () => false)
            return {destroy: () => destroyComputed(c)}
        },
    },
    'RxSet computed': {
        delivery: 'sync', create: (dep, body) => {
            const set = new RxSet(() => { body(); return [dep()] })
            return {destroy: () => set.destroy()}
        },
    },
    'RxList.map row': {
        delivery: 'sync', create: (dep, body) => {
            const mapped = new RxList([1]).map(item => { dep(); body(); return item })
            return {destroy: () => mapped.destroy()}
        },
    },
}

describe('a child cleanup that throws while its parent re-runs', () => {
    for (const [name, {delivery, create}] of Object.entries(HOSTS)) {
        function setup() {
            const dep = atom(0)
            const tick = atom(0)
            const control = {throws: false, runs: 0, childRuns: 0}
            const host = create(dep, () => {
                control.runs++
                // 抛错的子 effect 在前,正常的在后:后者照常销毁
                autorun(({onCleanup}) => { onCleanup(() => { if (control.throws) throw new Error('child cleanup') }) }, true)
                autorun(() => { tick(); control.childRuns++ }, true)
            })
            return {dep, tick, control, host}
        }
        async function expectRecovers({dep, tick, control, host}: ReturnType<typeof setup>) {
            control.throws = false
            const runs = control.runs
            dep(dep.raw + 1)
            await settle()
            expect(control.runs).toBe(runs + 1)
            const children = control.childRuns
            tick(tick.raw + 1)
            await settle()
            // 只剩这一轮创建的那个子 effect:失败那轮留下的没有泄漏
            expect(control.childRuns).toBe(children + 1)
            host.destroy()
            tick(tick.raw + 1)
            await settle()
            expect(control.childRuns).toBe(children + 1)
        }
        function expectDelivered(thrown: unknown) {
            if (delivery === 'sync') {
                expect((thrown as Error)?.message).toBe('child cleanup')
                expect(errorLog).toEqual([])
            } else {
                expect(thrown).toBe(undefined)
                expect(errorLog.length).toBe(1)
                expect((errorLog[0][1] as Error).message).toBe('child cleanup')
            }
        }

        test(`${name}: top level`, async () => {
            const env = setup()
            await settle()
            const before = trackingState()
            // 兄弟订阅者:同一次派发里照常执行
            let siblingRuns = 0
            const stopSibling = autorun(() => { env.dep(); siblingRuns++ }, true)

            env.control.throws = true
            let thrown: unknown
            try { env.dep(1) } catch (err) { thrown = err }
            expect(trackingState()).toEqual(before)
            await settle()
            expectDelivered(thrown)
            expect(trackingState()).toEqual(before)
            expect(siblingRuns).toBe(2)

            // 顶层读取不属于任何 effect
            const unrelated = atom(0)
            unrelated()
            expect(subscribers(unrelated)).toEqual([])
            // dep 上没有残留的 marker 位:之后同一层级的订阅不会被误判为已追踪
            let lateRuns = 0
            const stopLate = autorun(() => { env.dep(); lateRuns++ }, true)
            expect(subscribers(env.dep).length).toBe(3)

            await expectRecovers(env)
            expect(siblingRuns).toBe(3)
            expect(lateRuns).toBe(2)
            stopSibling()
            stopLate()
        })

        test(`${name}: inside another running effect`, async () => {
            const env = setup()
            await settle()
            const go = atom(0)
            const after = atom(0)
            let outerRuns = 0
            let thrown: unknown
            let states: unknown[] = []
            const stopOuter = autorun(() => {
                outerRuns++
                if (go() === 1 && !states.length) {
                    const before = trackingState()
                    try { env.dep(env.dep.raw + 1) } catch (err) { thrown = err }
                    states = [before, trackingState()]
                }
                after()
            }, true)
            env.control.throws = true
            go(1)
            expect(states[1]).toEqual(states[0])
            await settle()
            expectDelivered(thrown)
            // 抛错之后的读取仍属于外层 effect
            after(1)
            expect(outerRuns).toBe(3)
            stopOuter()
            await expectRecovers(env)
        })
    }

    test('inside batch: the other queued subscribers run and the first error reaches the caller', () => {
        const dep = atom(0)
        let shouldThrow = false
        let siblingRuns = 0
        const stop = autorun(() => {
            dep()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        }, true)
        const stopSibling = autorun(() => { dep(); siblingRuns++ }, true)
        shouldThrow = true
        expect(() => batch(() => { dep(1) })).toThrow('child cleanup')
        expect(siblingRuns).toBe(2)
        shouldThrow = false
        dep(2)
        expect(siblingRuns).toBe(3)
        stop()
        stopSibling()
    })

    test('deeper than the dep markers (full cleanup mode)', () => {
        const atoms = Array.from({length: 33}, () => atom(0))
        const stops = atoms.slice(0, -1).map((_, i) => autorun(() => {
            const value = atoms[i]()
            if (value) atoms[i + 1](value)
        }, true))
        let shouldThrow = false
        let runs = 0
        const stop = autorun(() => {
            runs++
            atoms[32]()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        }, true)
        shouldThrow = true
        expect(() => atoms[0](1)).toThrow('child cleanup')
        shouldThrow = false
        atoms[32](100)
        expect(runs).toBe(2)
        stop()
        stops.forEach(s => s())
    })

    test('full cleanup mode: a read in the throwing cleanup leaves no marker bit behind', () => {
        // patch 型 computed 不用依赖标记。清理在抛错前读了 x：x 的 dep 上不能留下本深度的
        // 标记位，否则之后在同一深度读 x 的 effect 会被判为已订阅，从此收不到 x 的变化。
        const source = atom(0)
        const x = atom(0)
        let shouldThrow = false
        const host = computed<number>(function (this: Computed) {
            this.manualTrack(source, TrackOpTypes.ATOM, 'value')
            autorun(({onCleanup}) => { onCleanup(() => { x(); if (shouldThrow) throw new Error('child cleanup') }) }, true)
            return source.raw
        }, () => false)
        host()
        shouldThrow = true
        expect(() => source(1)).toThrow('child cleanup')
        shouldThrow = false

        let runs = 0
        const stop = autorun(() => { x(); runs++ }, true)
        expect(subscribers(x)).toHaveLength(1)
        x(1)
        expect(runs).toBe(2)
        stop()
        destroyComputed(host)
    })

    test('an async getter settles like a generator getter', async () => {
        for (const getter of [async () => { body() }, function* (): any { body() }]) {
            var dep = atom(0)
            var shouldThrow = false
            const effect = new AtomComputed(getter, undefined, true)
            await settle()
            expect(effect.status.raw).toBe(STATUS_CLEAN)
            shouldThrow = true
            let outcome = 'pending'
            effect.recompute(true)!.then(() => { outcome = 'resolved' }, (err: Error) => { outcome = err.message })
            await settle()
            expect(outcome).toBe('child cleanup')
            expect(effect.asyncStatus!.raw).toBe(false)
            expect(effect.status.raw).toBe(STATUS_DIRTY)
            shouldThrow = false
            dep(1)
            await settle()
            expect(effect.status.raw).toBe(STATUS_CLEAN)
            effect.destroy()
        }
        function body() {
            dep()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        }
    })

    test('a parent destroyed by the throwing cleanup stays destroyed', () => {
        const hosts: Array<(dep: Atom<number>, body: () => void) => ReactiveEffect> = [
            (dep, body) => new LightEffect(() => { dep(); body() }),
            (dep, body) => {
                const effect = new Computed(() => { dep(); body() }, undefined, true)
                effect.run([], true)
                return effect
            },
        ]
        for (const create of hosts) {
            const dep = atom(0)
            let runs = 0
            const parent: ReactiveEffect = create(dep, () => {
                runs++
                autorun(({onCleanup}) => {
                    onCleanup(() => {
                        parent.destroy()
                        throw new Error('child cleanup')
                    })
                }, true)
            })
            expect(() => dep(1)).toThrow('child cleanup')
            expect(parent.active).toBe(false)
            expect(subscribers(dep)).toEqual([])
            dep(2)
            expect(runs).toBe(1)
        }
    })

    test('a subclass that calls prepareTracking itself', () => {
        const dep = atom(0)
        let shouldThrow = false
        class Manual extends ReactiveEffect {
            runs = 0
            constructor() {
                super(() => {})
                this.refresh()
            }
            run() {
                this.refresh()
            }
            refresh() {
                this.prepareTracking()
                notifier.enableTracking()
                try {
                    this.runs++
                    dep()
                    autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
                } finally {
                    notifier.resetTracking()
                    this.completeTracking()
                }
            }
        }
        const effect = new Manual()
        shouldThrow = true
        expect(() => dep(1)).toThrow('child cleanup')
        shouldThrow = false
        dep(2)
        expect(effect.runs).toBe(2)
        effect.destroy()
    })
})

/**
 * 清理抛错时销毁照常做完。
 *
 * 静态 destroy 先把 effect 置为 inactive 再执行用户清理。清理抛错若就此中断:它的订阅没摘、
 * 子 effect 没销毁、destroy 事件没派发,而之后任何销毁入口都会因为 inactive 跳过它——永久残留。
 * destroyChildren 同理:一个子 effect 抛错,后面的兄弟不能留下(宿主自己正在被销毁时没有下一轮)。
 * 第一个错误抛给调用方,收尾中再出的错误走 console.error。
 */
describe('a cleanup that throws does not leave the teardown half done', () => {
    test('the subtree of the child whose cleanup threw is destroyed', () => {
        const dep = atom(0)
        const tick = atom(0)
        let shouldThrow = false
        let grandRuns = 0
        const stop = autorun(() => {
            dep()
            autorun(({onCleanup}) => {
                onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') })
                autorun(() => { tick(); grandRuns++ }, true)
            }, true)
        }, true)
        shouldThrow = true
        expect(() => dep(1)).toThrow('child cleanup')
        shouldThrow = false
        // 上一轮的孙 effect 已随抛错的子 effect 销毁;宿主保留依赖,下一次触发恢复
        expect(subscribers(tick)).toEqual([])
        dep(2)
        expect(subscribers(tick)).toHaveLength(1)
        stop()
        expect(subscribers(tick)).toEqual([])
        const before = grandRuns
        tick(1)
        expect(grandRuns).toBe(before)
    })

    test('the effect whose own cleanup threw is unsubscribed, loses its children and reports destroy', () => {
        const dep = atom(0)
        const tick = atom(0)
        let childRuns = 0
        let destroyed = 0
        const c = new Computed(function (this: Computed, {onCleanup}: any) {
            onCleanup(() => { throw new Error('own cleanup') })
            autorun(() => { tick(); childRuns++ }, true)
            return dep()
        }, undefined, true)
        c.run([], true)
        c.on('destroy', () => { destroyed++ })
        expect(subscribers(dep)).toHaveLength(1)

        expect(() => c.destroy()).toThrow('own cleanup')
        expect(c.active).toBe(false)
        expect(subscribers(dep)).toEqual([])
        expect(subscribers(tick)).toEqual([])
        expect(c.hasChildren()).toBe(false)
        expect(destroyed).toBe(1)
        tick(1)
        expect(childRuns).toBe(1)
        // 幂等:再销毁一次什么都不做
        expect(() => c.destroy()).not.toThrow()
        expect(destroyed).toBe(1)
    })

    test('a child destroyed on its own whose cleanup throws still leaves its parent', () => {
        let child!: Computed
        let siblingCleanups = 0
        let shouldThrow = true
        const dep = atom(0)
        const parent = new Computed(() => {
            dep()
            child = new Computed(({onCleanup}: any) => { onCleanup(() => { if (shouldThrow) throw new Error('own cleanup') }); return 1 }, undefined, true)
            child.run([], true)
            autorun(({onCleanup}) => { onCleanup(() => { siblingCleanups++ }) }, true)
        }, undefined, true)
        parent.run([], true)
        expect(parent.children.length).toBe(2)

        expect(() => child.destroy()).toThrow('own cleanup')
        shouldThrow = false
        expect(child.parent).toBeUndefined()
        expect(parent.children.length).toBe(1)
        // 父 effect 的记账没被留下的条目弄乱:重算只清理还活着的那个兄弟
        dep(1)
        expect(siblingCleanups).toBe(1)
        parent.destroy()
    })

    test('destroying a parent: the siblings after a throwing child are destroyed, the first error is thrown', () => {
        const tick = atom(0)
        const cleaned: string[] = []
        const runs: Record<string, number> = {b: 0, c: 0}
        const stop = autorun(() => {
            autorun(({onCleanup}) => { onCleanup(() => { cleaned.push('a'); throw new Error('first') }) }, true)
            autorun(({onCleanup}) => { tick(); runs.b++; onCleanup(() => { cleaned.push('b'); throw new Error('second') }) }, true)
            autorun(({onCleanup}) => { tick(); runs.c++; onCleanup(() => { cleaned.push('c') }) }, true)
        }, true)
        expect(subscribers(tick)).toHaveLength(2)

        expect(() => stop()).toThrow('first')
        expect(cleaned).toEqual(['a', 'b', 'c'])
        expect(subscribers(tick)).toEqual([])
        // 第二个错误不覆盖第一个,走 console.error
        expect(errorLog).toHaveLength(1)
        expect(String(errorLog[0][0])).toContain('teardown')
        expect((errorLog[0][1] as Error).message).toBe('second')
        tick(1)
        expect(runs).toEqual({b: 1, c: 1})
    })

    test('a destroy listener that throws after the teardown: nothing is run twice', () => {
        const dep = atom(0)
        let cleanups = 0
        let events = 0
        const c = new Computed(function (this: Computed, {onCleanup}: any) {
            onCleanup(() => { cleanups++ })
            return dep()
        }, undefined, true)
        c.run([], true)
        c.on('destroy', () => { events++; throw new Error('listener') })
        expect(() => c.destroy()).toThrow('listener')
        expect(cleanups).toBe(1)
        expect(events).toBe(1)
        expect(subscribers(dep)).toEqual([])
        expect(errorLog).toEqual([])
    })

    test('a destroy listener that throws does not keep the later listeners from being notified', () => {
        const dep = atom(0)
        const notified: string[] = []
        const c = new Computed(() => dep(), undefined, true, {
            onDestroy() { notified.push('callbacks.onDestroy'); throw new Error('first') },
        })
        c.run([], true)
        c.on('destroy', () => { notified.push('second'); throw new Error('second') })
        c.on('destroy', () => { notified.push('third') })

        expect(() => c.destroy()).toThrow('first')
        expect(notified).toEqual(['callbacks.onDestroy', 'second', 'third'])
        expect(subscribers(dep)).toEqual([])
        expect(errorLog).toHaveLength(1)
        expect((errorLog[0][1] as Error).message).toBe('second')
    })

    test('explicit destroy inside another effect: tracking is restored and that effect is not subscribed', () => {
        const dep = atom(0)
        const read = atom(0)
        const go = atom(0)
        const c = computed(({onCleanup}) => { onCleanup(() => { read(); throw new Error('own cleanup') }); return dep() })
        c()
        let outerRuns = 0
        let caught: unknown
        const stop = autorun(() => {
            outerRuns++
            if (go()) {
                try { destroyComputed(c) } catch (e) { caught = e }
            }
        }, true)
        go(1)
        expect((caught as Error).message).toBe('own cleanup')
        expect(subscribers(dep)).toEqual([])
        expect(subscribers(read)).toEqual([])
        read(1)
        expect(outerRuns).toBe(2)
        stop()
    })
})

describe('a bare effect in full cleanup mode keeps its dependencies', () => {
    // full cleanup 模式在销毁子 effect 之前就清空依赖。Computed 有自己的错误恢复,
    // 裸 effect 没有:prepareTracking 自己用快照恢复,否则它从此不再响应。
    class FullCleanupEffect extends LightEffect {
        constructor(fn: () => any) {
            super(fn)
        }
    }
    FullCleanupEffect.prototype.useDepMarker = false

    test('useDepMarker = false', () => {
        const dep = atom(0)
        const other = atom(0)
        let runs = 0
        let shouldThrow = false
        const e = new FullCleanupEffect(() => {
            runs++
            dep()
            other()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        })
        expect(e.useDepMarker).toBe(false)
        shouldThrow = true
        expect(() => dep(1)).toThrow('child cleanup')
        expect(subscribers(dep)).toEqual([e])
        expect(subscribers(other)).toEqual([e])
        shouldThrow = false
        other(1)
        expect(runs).toBe(2)
        e.destroy()
        expect(subscribers(dep)).toEqual([])
    })

    test('deeper than the dep markers', () => {
        const atoms = Array.from({length: 33}, () => atom(0))
        const stops = atoms.slice(0, -1).map((_, i) => autorun(() => {
            const value = atoms[i]()
            if (value) atoms[i + 1](value)
        }, true))
        let shouldThrow = false
        let runs = 0
        const e = new LightEffect(() => {
            runs++
            atoms[32]()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) throw new Error('child cleanup') }) }, true)
        })
        shouldThrow = true
        expect(() => atoms[0](1)).toThrow('child cleanup')
        shouldThrow = false
        expect(subscribers(atoms[32])).toEqual([e])
        atoms[32](100)
        expect(runs).toBe(2)
        e.destroy()
        stops.forEach(s => s())
    })

    test('a host destroyed by the throwing cleanup stays destroyed and unsubscribed', () => {
        const dep = atom(0)
        let runs = 0
        let shouldThrow = false
        const e: FullCleanupEffect = new FullCleanupEffect(() => {
            runs++
            dep()
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) { e.destroy(); throw new Error('child cleanup') } }) }, true)
        })
        shouldThrow = true
        expect(() => dep(1)).toThrow('child cleanup')
        expect(e.active).toBe(false)
        expect(subscribers(dep)).toEqual([])
        dep(2)
        expect(runs).toBe(1)
    })
})

/**
 * 子 effect 的 destroy() 覆写在走到静态核心之前就抛错（review 发现，本 PR 的 c3d3312 引入）。
 *
 * destroyChildren 无论成败都丢弃这一代 children 数组。覆写在 super.destroy() 之前抛错时，
 * 这个子 effect 仍是 active、仍订阅着依赖，还留着 parent 与旧数组的 index——成为活着的孤儿：
 * 没有人会再销毁它；之后被单独销毁时按陈旧 index 在新 children 上 swap-pop，删掉活着的兄弟
 * （与「清理里创建的 effect 弄坏 children 记账」同一缺陷类）。
 * 等价类：{覆写在 super 之前抛错} × {第一个抛错者 / 兄弟先抛错之后的续销毁循环}。
 */
describe('a child whose destroy() override throws before reaching the core', () => {
    class OverridingComputed extends Computed {
        throwOnce = false
        destroy(ignoreChildren = false, fromParent = false) {
            if (this.throwOnce) {
                this.throwOnce = false
                throw new Error('override teardown')
            }
            super.destroy(ignoreChildren, fromParent)
        }
    }

    function setup(withThrowingFirstSibling = false) {
        const tick = atom(0)
        const depK = atom(0)
        const depS = atom(0)
        const generations: Array<{k: OverridingComputed, s: Computed}> = []
        let kRuns = 0
        let firstThrows = false
        let parent!: ReactiveEffect
        const stop = autorun(() => {
            tick()
            parent = ReactiveEffect.activeScopes[ReactiveEffect.activeScopes.length - 1]
            if (withThrowingFirstSibling) {
                autorun(({onCleanup}) => { onCleanup(() => { if (firstThrows) throw new Error('first sibling') }) }, true)
            }
            const k = new OverridingComputed(() => { depK(); kRuns++ }, undefined, true)
            k.run([], true)
            const s = new Computed(() => { depS() }, undefined, true)
            s.run([], true)
            generations.push({k, s})
        }, true)
        return {
            tick, depK, depS, generations, stop,
            parent: () => parent,
            kRuns: () => kRuns,
            armFirstSibling: (value: boolean) => { firstThrows = value },
        }
    }

    test('the child is destroyed anyway: unsubscribed, not re-run, the error reaches the caller', () => {
        const {tick, depK, generations, stop, kRuns} = setup()
        const k0 = generations[0].k
        k0.throwOnce = true
        expect(() => tick(1)).toThrow('override teardown')
        expect(k0.active).toBe(false)
        expect(k0.parent).toBeUndefined()
        // 父 effect 这一轮在 prepareTracking 就失败了，没有新一代：k0 不能再跑
        let runsBefore = kRuns()
        depK(1)
        expect(kRuns() - runsBefore).toBe(0)
        // 下一轮重算产生新一代，只有它响应
        tick(2)
        runsBefore = kRuns()
        depK(2)
        expect(kRuns() - runsBefore).toBe(1)
        stop()
        expect(subscribers(depK)).toEqual([])
    })

    test('destroying it again later does not evict a live sibling of the new generation', () => {
        const {tick, depK, depS, generations, stop, parent} = setup()
        const k0 = generations[0].k
        k0.throwOnce = true
        expect(() => tick(1)).toThrow('override teardown')
        tick(2)
        const latest = generations[generations.length - 1]
        k0.destroy()
        expect((parent() as any)._children).toContain(latest.k)
        expect((parent() as any)._children).toContain(latest.s)
        stop()
        expect(latest.k.active).toBe(false)
        expect(latest.s.active).toBe(false)
        expect(subscribers(depK)).toEqual([])
        expect(subscribers(depS)).toEqual([])
    })

    test('after an earlier sibling threw: the error is reported and the child is still destroyed', () => {
        const {tick, depK, generations, stop, armFirstSibling} = setup(true)
        armFirstSibling(true)
        generations[0].k.throwOnce = true
        expect(() => tick(1)).toThrow('first sibling')
        armFirstSibling(false)
        expect(generations[0].k.active).toBe(false)
        expect(errorLog.map(args => (args[1] as Error).message)).toEqual(['override teardown'])
        stop()
        expect(subscribers(depK)).toEqual([])
    })
})

describe('error precedence when the teardown fails in more than one place', () => {
    test('a host whose own cleanup and a child both throw: the host error wins, every listener is notified', () => {
        const tick = atom(0)
        const notified: string[] = []
        const host = new Computed(function (this: Computed, {onCleanup}: any) {
            onCleanup(() => { throw new Error('host cleanup') })
            autorun(({onCleanup}) => { tick(); onCleanup(() => { throw new Error('child cleanup') }) }, true)
            autorun(({onCleanup}) => { tick(); onCleanup(() => { notified.push('sibling cleanup') }) }, true)
        }, undefined, true)
        host.run([], true)
        host.on('destroy', () => { notified.push('host listener 1'); throw new Error('host listener') })
        host.on('destroy', () => { notified.push('host listener 2') })
        expect(subscribers(tick)).toHaveLength(2)

        expect(() => host.destroy()).toThrow('host cleanup')
        expect(host.active).toBe(false)
        expect(subscribers(tick)).toEqual([])
        expect(notified).toEqual(['sibling cleanup', 'host listener 1', 'host listener 2'])
        expect(errorLog.map(args => (args[1] as Error).message)).toEqual(['child cleanup', 'host listener'])
    })

    test('a child cleanup that destroys its re-running parent and throws: its error is not replaced by the parent teardown', () => {
        const hosts: Array<(dep: Atom<number>, body: () => void) => ReactiveEffect> = [
            (dep, body) => new LightEffect(() => { dep(); body() }),
            (dep, body) => {
                const effect = new Computed(() => { dep(); body() }, undefined, true)
                effect.run([], true)
                return effect
            },
        ]
        for (const create of hosts) {
            errorLog.length = 0
            const dep = atom(0)
            let armed = false
            const parent: ReactiveEffect = create(dep, () => {
                autorun(({onCleanup}) => {
                    onCleanup(() => {
                        if (!armed) return
                        parent.destroy()
                        throw new Error('child cleanup')
                    })
                }, true)
            })
            parent.on('destroy', () => { if (armed) throw new Error('parent destroy listener') })
            armed = true
            // 父 effect 的延迟销毁在 prepareTracking 撤销时执行，它的错误是第二个错误
            expect(() => dep(1)).toThrow('child cleanup')
            expect(errorLog.map(args => (args[1] as Error).message)).toEqual(['parent destroy listener'])
            expect(parent.active).toBe(false)
            expect(subscribers(dep)).toEqual([])
        }
    })
})
