/**
 * 清理不建立依赖:清理代码运行在「恰好正在执行的 effect」的帧里,其中的读取不属于那个 effect。
 *
 * 等价类:{被清理者的清理形态} × {触发清理的入口} × {此刻在作用域栈顶的 effect}。
 *   入口 1 — 父 effect 重算:prepareTracking 先把父 effect 压栈,再销毁上一轮的子 effect;
 *   入口 2 — 重算钩子:computed 在别的 effect 执行中被重算,自己上一轮的清理跑在外层帧里;
 *   入口 3 — 显式销毁:在 effect 执行中调用 stop()/destroy()/destroyComputed(),含栈内销毁的延迟执行;
 *   入口 4 — map 行重算:行级 context.onCleanup 在行 effect 自己的 getter 里执行。
 * 泄漏的后果不只是多跑一次:清理里读、挂载时写同一个 atom 就是无限重算;patch 型结构
 * 被订阅到无关 atom 上,之后的写入会向 applyPatch 交付协议外的 info。
 *
 * 防线:入口 × 形态 × 父 effect 的全叉乘矩阵 + 「清理里的追踪读取 ≡ .raw 读取」差分 fuzz。
 */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest'
import {
    atom,
    Atom,
    autorun,
    batch,
    Computed,
    computed,
    destroyComputed,
    notifier,
    ReactiveEffect,
    RxList,
    RxMap,
    RxSet,
    TrackOpTypes,
} from '../src/index.js'
import {mulberry32} from './fuzzKit.js'

const subscribers = (target: object) => [...(notifier.getDepEffects(target) ?? [])]

// 默认调度的 autorun 在微任务里重跑,可能再次触发自己,多等几轮才能看到收敛后的状态
async function settle() {
    for (let i = 0; i < 100; i++) await Promise.resolve()
}

beforeEach(() => {
    // async getter 的形态提示与本文件无关
    vi.spyOn(console, 'warn').mockImplementation(() => {})
})

afterEach(() => {
    vi.restoreAllMocks()
    expect(ReactiveEffect.activeScopes.length).toBe(0)
    expect(notifier.trackStack.length).toBe(0)
    expect(notifier.effectTrackDepth).toBe(0)
    expect(notifier.shouldTrack).toBe(true)
})

// 与 axii 的轻量绑定同形:直接继承 ReactiveEffect,不经过 Computed 的重算状态机
class LightEffect extends ReactiveEffect {
    constructor(fn: () => any) {
        super(fn)
        this.run()
    }
    callGetter() {
        return this.getter!()
    }
    run() {
        // 基类对 inactive 的 run 会退化成直接调用 getter(派发快照里可能还有已销毁的 effect)
        if (!this.active) return
        return super.run()
    }
}

// ---- 清理形态:每一项建立一个「销毁时会执行用户代码」的结构,返回销毁它的方式 ----
type Teardown = (probe: () => void) => () => void

const TEARDOWNS: Record<string, Teardown> = {
    'autorun context.onCleanup': probe => autorun(({onCleanup}) => { onCleanup(probe) }, true),
    'computed context.onCleanup': probe => {
        const c = computed<number>(({onCleanup}) => { onCleanup(probe); return 1 })
        return () => destroyComputed(c)
    },
    'async computed context.onCleanup': probe => {
        const c = computed<any>(async ({onCleanup}) => { onCleanup(probe) }, undefined, true)
        return () => destroyComputed(c)
    },
    'computed callbacks.onDestroy': probe => {
        const c = computed(() => 1, undefined, undefined, {onDestroy: probe})
        return () => destroyComputed(c)
    },
    'RxMap callbacks.onDestroy': probe => {
        const map = new RxMap(() => [['a', 1]] as [string, number][], undefined, undefined, {onDestroy: probe})
        return () => map.destroy()
    },
    'on("destroy") listener of a source structure': probe => {
        const list = new RxList([1])
        list.on('destroy', probe)
        return () => list.destroy()
    },
    'grandchild context.onCleanup': probe => autorun(() => {
        autorun(() => { autorun(({onCleanup}) => { onCleanup(probe) }, true) }, true)
    }, true),
    'map row context.onCleanup': probe => {
        const mapped = new RxList([1, 2]).map((item, _index, {onCleanup}) => { onCleanup(probe); return item })
        return () => mapped.destroy()
    },
    'map options.onCleanup': probe => {
        const mapped = new RxList([1, 2]).map(item => item, {onCleanup: () => probe()})
        return () => mapped.destroy()
    },
    'child effect of a map row': probe => {
        const mapped = new RxList([1]).map(item => {
            autorun(({onCleanup}) => { onCleanup(probe) }, true)
            return item
        })
        return () => mapped.destroy()
    },
    'destroyResources override': probe => {
        class Resource extends Computed {
            destroyResources() {
                probe()
                super.destroyResources()
            }
        }
        const resource = new Resource(() => 1)
        return () => resource.destroy()
    },
}

// ---- 宿主形态:body 是它的重算体,rerun 让它重算恰好一轮 ----
type Host = {rerun: () => void, runs: () => number, destroy: () => void}
type HostFactory = (body: () => void) => Host

const HOSTS: Record<string, HostFactory> = {
    'autorun': body => {
        const dep = atom(0)
        let runs = 0
        const destroy = autorun(() => { runs++; dep(); body() }, true)
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy}
    },
    'computed': body => {
        const dep = atom(0)
        let runs = 0
        const c = computed(() => { runs++; body(); return dep() })
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => destroyComputed(c)}
    },
    'light effect (no Computed state machine)': body => {
        const dep = atom(0)
        let runs = 0
        const effect = new LightEffect(() => { runs++; dep(); body() })
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => effect.destroy()}
    },
    'async getter computed': body => {
        const dep = atom(0)
        let runs = 0
        const c = computed<any>(async () => { runs++; body(); return dep() }, undefined, true)
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => destroyComputed(c)}
    },
    'generator getter computed': body => {
        const dep = atom(0)
        let runs = 0
        const c = computed<number>(function* (): any { runs++; body(); return dep() }, undefined, true)
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => destroyComputed(c)}
    },
    'patch computed falling back to a full recompute': body => {
        const dep = atom(0)
        let runs = 0
        const c = computed<number>(function (this: Computed) {
            runs++
            this.manualTrack(dep, TrackOpTypes.ATOM, 'value')
            body()
            return dep.raw
        }, () => false)
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => destroyComputed(c)}
    },
    'RxSet computed': body => {
        const dep = atom(0)
        let runs = 0
        const set = new RxSet(() => { runs++; body(); return [dep()] })
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => set.destroy()}
    },
    'RxList.map full recompute': body => {
        let runs = 0
        const mapped = new RxList([1]).map(item => { runs++; body(); return item })
        return {rerun: () => { mapped.recompute(true) }, runs: () => runs, destroy: () => mapped.destroy()}
    },
    'RxList.map row': body => {
        const dep = atom(0)
        let runs = 0
        const mapped = new RxList([1]).map(item => { runs++; body(); return item + dep() })
        return {rerun: () => dep(dep.raw + 1), runs: () => runs, destroy: () => mapped.destroy()}
    },
    // 超过 maxMarkerBits(30)层后走 full cleanup 模式的另一条 track 分支
    'autorun nested deeper than the dep markers': body => {
        const atoms = Array.from({length: 33}, () => atom(0))
        let runs = 0
        const stops = atoms.slice(0, -1).map((_, i) => autorun(() => {
            const value = atoms[i]()
            if (value) atoms[i + 1](value)
        }, true))
        let version = 0
        const stop = autorun(() => { runs++; atoms[32](); body() }, true)
        return {
            rerun: () => atoms[0](++version),
            runs: () => runs,
            destroy: () => { stop(); stops.forEach(s => s()) },
        }
    },
}

describe('entry 1: re-running an effect tears down the children of its last run', () => {
    for (const [hostName, createHost] of Object.entries(HOSTS)) {
        for (const [teardownName, create] of Object.entries(TEARDOWNS)) {
            test(`${hostName} / ${teardownName}`, async () => {
                const read = atom(0)
                let probes = 0
                const host = createHost(() => { create(() => { probes++; read() }) })
                expect(probes).toBe(0)
                const before = host.runs()

                host.rerun()
                expect(host.runs()).toBe(before + 1)
                expect(probes).toBeGreaterThan(0)
                expect(subscribers(read)).toEqual([])

                // 写入清理读过的 atom:宿主不重跑,也不抛错(map 的宿主曾收到协议外的 info)
                read(1)
                expect(host.runs()).toBe(before + 1)

                // 宿主自己的依赖仍然有效
                host.rerun()
                expect(host.runs()).toBe(before + 2)
                expect(subscribers(read)).toEqual([])
                host.destroy()
                await settle()
            })
        }
    }

    test('a destroy() override of a child', () => {
        const dep = atom(0)
        const read = atom(0)
        let destroyed = 0
        class Custom extends Computed {
            destroy(ignoreChildren = false, fromParent = false) {
                destroyed++
                read()
                super.destroy(ignoreChildren, fromParent)
            }
        }
        const stop = autorun(() => { dep(); new Custom(() => 1) }, true)
        dep(1)
        expect(destroyed).toBe(1)
        expect(subscribers(read)).toEqual([])
        stop()
    })

    test('every kind of reactive read', () => {
        const dep = atom(0)
        const primitive = atom(0)
        const object = atom({x: 1})
        const derived = computed(() => primitive() + 1)
        const list = new RxList([1, 2])
        const map = new RxMap<string, number>([['a', 1]])
        const set = new RxSet([1])
        let probes = 0
        const host = new Computed(() => {
            dep()
            autorun(({onCleanup}) => {
                onCleanup(() => {
                    probes++
                    primitive(); object(); object.x; derived(); `${primitive}`
                    list.at(0); list.length(); list.forEach(() => {}); for (const _ of list) { /* iterate */ }
                    map.get('a'); map.size(); map.forEach(() => {})
                    set.has(1); set.size(); set.forEach(() => {})
                })
            }, true)
        }, undefined, true)
        host.run([], true)
        const deps = [...host.deps]
        expect(deps.length).toBe(1)

        dep(1)
        expect(probes).toBe(1)
        expect(host.deps).toEqual(deps)

        host.destroy()
        destroyComputed(derived)
        list.destroy(); map.destroy(); set.destroy()
    })

    test('reads of the body stay dependencies, also when the teardown reads the same atom', () => {
        const dep = atom(0)
        const shared = atom(0)
        let runs = 0
        const stop = autorun(() => {
            runs++
            dep()
            shared()
            autorun(({onCleanup}) => { onCleanup(() => { shared() }) }, true)
        }, true)
        dep(1)
        expect(subscribers(shared).length).toBe(1)
        shared(1)
        expect(runs).toBe(3)
        stop()
    })

    test('a dependency the body stopped reading is dropped, although the teardown reads it', () => {
        const toggle = atom(true)
        const branch = atom(0)
        let runs = 0
        const stop = autorun(() => {
            runs++
            if (toggle()) branch()
            autorun(({onCleanup}) => { onCleanup(() => { branch() }) }, true)
        }, true)
        expect(subscribers(branch).length).toBe(1)
        toggle(false)
        expect(runs).toBe(2)
        expect(subscribers(branch)).toEqual([])
        branch(1)
        expect(runs).toBe(2)
        stop()
    })

    test('releasing on cleanup what the next child claims does not re-run the parent forever', async () => {
        const active = atom<number | null>(null)
        const version = atom(0)
        let next = 0
        let mounts = 0
        const stop = autorun(() => {
            version()
            const id = ++next
            // 熔断:未修复时每轮微任务都会再触发一轮
            if (++mounts > 20) return
            autorun(({onCleanup}) => {
                active(id)
                onCleanup(() => { if (active() === id) active(null) })
            }, true)
        })
        version(1)
        await settle()
        expect(mounts).toBe(2)
        expect(active.raw).toBe(2)
        stop()
        expect(active.raw).toBe(null)
    })

    test('a mapped list is not subscribed to what the children of its rows read while being replaced', () => {
        const read = atom(0)
        const source = new RxList([1, 2])
        let builds = 0
        // 行使用 index:一次 batch 里多条 info 必然回退全量重算,prepareTracking 销毁旧行的子 effect
        const mapped = source.map((item, index) => {
            builds++
            autorun(({onCleanup}) => { onCleanup(() => { read() }) }, true)
            return item * 10 + index.raw
        })
        batch(() => { source.push(3); source.push(4) })
        expect(builds).toBe(2 + 4)
        expect(subscribers(read)).toEqual([])
        // 未修复时:mapped 成为 read 的订阅者,这次写入在 dev 抛 'unexpected triggerInfo source'
        expect(() => read(1)).not.toThrow()
        expect(mapped.data).toEqual([10, 21, 32, 43])
        mapped.destroy()
        source.destroy()
    })
})

describe('entry 2: recompute hooks run inside another running effect', () => {
    // hook 形态:host 的 getter 每轮都注册/持有一个会读 atom 的重算钩子
    const HOOKS: Record<string, (probe: () => void) => {register: (ctx: any) => void, callbacks?: any}> = {
        'context.onCleanup': probe => ({register: ctx => ctx.onCleanup(probe)}),
        'callbacks.onCleanup': probe => ({register: () => {}, callbacks: {onCleanup: probe}}),
        'callbacks.onRecompute': probe => ({register: () => {}, callbacks: {onRecompute: probe}}),
    }
    const MODES: Record<string, (dep: Atom<number>, register: (ctx: any) => void, callbacks: any) => () => void> = {
        'sync': (dep, register, callbacks) => {
            const c = computed<number>(context => { register(context); return dep() }, undefined, undefined, callbacks)
            return () => destroyComputed(c)
        },
        'sync patch': (dep, register, callbacks) => {
            const c = computed<number>(function (this: Computed, context) {
                register(context)
                this.manualTrack(dep, TrackOpTypes.ATOM, 'value')
                return dep.raw
            }, data => { (data as Atom<number>)(dep.raw) }, undefined, callbacks)
            return () => destroyComputed(c)
        },
        'async getter': (dep, register, callbacks) => {
            const c = computed<any>(async context => { register(context); return dep() }, undefined, true, callbacks)
            return () => destroyComputed(c)
        },
        'generator getter': (dep, register, callbacks) => {
            const c = computed<number>(function* (context): any { register(context); return dep() }, undefined, true, callbacks)
            return () => destroyComputed(c)
        },
        'RxList': (dep, register, callbacks) => {
            const list = new RxList<number>(context => { register(context); return [dep()] }, undefined, undefined, callbacks)
            return () => list.destroy()
        },
    }
    const OUTERS: Record<string, (body: () => void) => () => void> = {
        'autorun': body => autorun(body, true),
        'light effect': body => {
            const effect = new LightEffect(body)
            return () => effect.destroy()
        },
        'computed': body => {
            const c = computed(body)
            return () => destroyComputed(c)
        },
    }
    for (const [outerName, createOuter] of Object.entries(OUTERS)) {
        for (const [modeName, createInner] of Object.entries(MODES)) {
            for (const [hookName, createHook] of Object.entries(HOOKS)) {
                test(`${outerName} / ${modeName} / ${hookName}`, async () => {
                    const dep = atom(0)
                    const read = atom(0)
                    const go = atom(0)
                    let probes = 0
                    let outerRuns = 0
                    const {register, callbacks} = createHook(() => { probes++; read() })
                    const destroyInner = createInner(dep, register, callbacks)
                    await settle()
                    const initialProbes = probes

                    const stop = createOuter(() => {
                        outerRuns++
                        if (go()) dep(dep.raw + 1)
                    })
                    go(1)
                    expect(outerRuns).toBe(2)
                    expect(probes).toBe(initialProbes + 1)
                    expect(subscribers(read)).toEqual([])
                    read(1)
                    expect(outerRuns).toBe(2)

                    stop()
                    destroyInner()
                    await settle()
                })
            }
        }
    }
})

describe('entry 3: destroying inside another running effect', () => {
    for (const [teardownName, create] of Object.entries(TEARDOWNS)) {
        test(`explicit destroy / ${teardownName}`, async () => {
            const read = atom(0)
            const go = atom(0)
            let probes = 0
            let outerRuns = 0
            const destroy = create(() => { probes++; read() })
            const stop = autorun(() => {
                outerRuns++
                if (go()) destroy()
            }, true)
            expect(probes).toBe(0)
            go(1)
            expect(probes).toBeGreaterThan(0)
            expect(outerRuns).toBe(2)
            expect(subscribers(read)).toEqual([])
            read(1)
            expect(outerRuns).toBe(2)
            stop()
            await settle()
        })
    }

    test('a destroy deferred to the end of the own run', () => {
        const read = atom(0)
        const go = atom(0)
        const trigger = atom(0)
        let probes = 0
        let outerRuns = 0
        const inner = autorun(({onCleanup}) => {
            onCleanup(() => { probes++; read() })
            if (trigger()) inner()
        }, true)
        const stop = autorun(() => {
            outerRuns++
            if (go()) trigger(1)
        }, true)
        probes = 0
        go(1)
        // 一次是重算前的清理,一次是本轮结束后延迟执行的销毁
        expect(probes).toBe(2)
        expect(outerRuns).toBe(2)
        expect(subscribers(read)).toEqual([])
        read(1)
        expect(outerRuns).toBe(2)
        stop()
    })

    test('AsyncRxSlice-style destroyResources writes still notify their subscribers with tracking', () => {
        const state = atom(1)
        const other = atom(0)
        const go = atom(0)
        let watcherRuns = 0
        class Resource extends Computed {
            destroyResources() {
                state(0)
                super.destroyResources()
            }
        }
        const resource = new Resource(() => 1)
        const stopWatcher = autorun(() => {
            watcherRuns++
            if (state() === 0) other()
        }, true)
        const stop = autorun(() => { if (go()) resource.destroy() }, true)
        go(1)
        expect(watcherRuns).toBe(2)
        expect(subscribers(other).length).toBe(1)
        other(1)
        expect(watcherRuns).toBe(3)
        stop()
        stopWatcher()
    })
})

describe('entry 3b: a destroy() override reached through destroyComputed', () => {
    // destroy() 覆写里的代码先于静态核心执行。经 destroyChildren 销毁时已被隔离,
    // 经 destroyComputed 显式销毁时同样不能订阅正在执行的 effect。
    for (const [outerName, createOuter] of Object.entries({
        'autorun': (body: () => void) => autorun(body, true),
        'computed': (body: () => void) => { const c = computed(body); return () => destroyComputed(c) },
    })) {
        test(outerName, () => {
            const read = atom(0)
            const go = atom(0)
            let probes = 0
            class Resource extends Computed {
                destroy(ignoreChildren = false, fromParent = false) {
                    probes++
                    read()
                    super.destroy(ignoreChildren, fromParent)
                }
            }
            const resource = new Resource(() => 1)
            let outerRuns = 0
            const stop = createOuter(() => {
                outerRuns++
                if (go()) destroyComputed(resource)
            })
            go(1)
            expect(probes).toBe(1)
            expect(subscribers(read)).toEqual([])
            read(1)
            expect(outerRuns).toBe(2)
            stop()
        })
    }
})

describe('entry 4: re-running a map row', () => {
    test('the row context.onCleanup of its last run does not subscribe the row', () => {
        const dep = atom(0)
        const read = atom(0)
        let runs = 0
        let probes = 0
        const source = new RxList([1, 2])
        const mapped = source.map((item, _index, {onCleanup}) => {
            runs++
            onCleanup(() => { probes++; read() })
            return item + dep()
        })
        dep(1)
        expect(probes).toBe(2)
        expect(runs).toBe(4)
        expect(subscribers(read)).toEqual([])
        read(1)
        expect(runs).toBe(4)
        dep(2)
        expect(runs).toBe(6)
        expect(mapped.data).toEqual([3, 4])
        mapped.destroy()
        source.destroy()
    })

    test('the same when the row re-runs before a pending structural patch', () => {
        const dep = atom(0)
        const read = atom(0)
        let probes = 0
        const source = new RxList([1, 2])
        const mapped = source.map((item, _index, {onCleanup}) => {
            onCleanup(() => { probes++; read() })
            return item + dep()
        })
        batch(() => {
            dep(1)
            source.splice(0, 0, 0)
        })
        expect(probes).toBeGreaterThan(0)
        expect(subscribers(read)).toEqual([])
        expect(mapped.data).toEqual([1, 2, 3])
        mapped.destroy()
        source.destroy()
    })

    test('tracking is restored when the row cleanup throws', () => {
        const dep = atom(0)
        const source = new RxList([1])
        let shouldThrow = true
        const mapped = source.map((item, _index, {onCleanup}) => {
            onCleanup(() => { if (shouldThrow) throw new Error('row cleanup') })
            return item + dep()
        })
        expect(() => dep(1)).toThrow('row cleanup')
        expect(notifier.trackStack.length).toBe(0)
        expect(notifier.shouldTrack).toBe(true)
        // 恢复探针:下一次写入照常重算该行
        shouldThrow = false
        dep(2)
        expect(mapped.data).toEqual([3])
        mapped.destroy()
        source.destroy()
    })
})

describe('what teardown code starts is still tracked', () => {
    test('an effect triggered by a write in a cleanup collects its dependencies', () => {
        const dep = atom(0)
        const released = atom(0)
        const late = atom(0)
        let watcherRuns = 0
        const stopWatcher = autorun(() => {
            watcherRuns++
            if (released()) late()
        }, true)
        const light = new LightEffect(() => { if (released()) late() })
        const stop = autorun(() => {
            dep()
            autorun(({onCleanup}) => { onCleanup(() => { released(released.raw + 1) }) }, true)
        }, true)
        dep(1)
        expect(watcherRuns).toBe(2)
        expect(subscribers(late).length).toBe(2)
        late(1)
        expect(watcherRuns).toBe(3)
        stop()
        stopWatcher()
        light.destroy()
    })

    test('structures created in a cleanup react to their own sources', () => {
        const dep = atom(0)
        const source = new RxList([1, 2, 3])
        const value = atom(1)
        let created: {evens: RxList<number>, length: Atom<number>, double: Atom<number>, log: number[], stop: () => void} | undefined
        const stop = autorun(() => {
            if (dep() !== 0) return
            autorun(({onCleanup}) => {
                onCleanup(() => {
                    const log: number[] = []
                    created = {
                        evens: source.filter(x => x % 2 === 0),
                        length: source.length,
                        double: computed(() => value() * 2),
                        stop: autorun(() => { log.push(value()) }, true),
                        log,
                    }
                })
            }, true)
        }, true)
        dep(1)
        source.push(4)
        value(2)
        expect(created!.evens.data).toEqual([2, 4])
        expect(created!.length()).toBe(4)
        expect(created!.double()).toBe(4)
        expect(created!.log).toEqual([1, 2])
        // 父 effect 再重跑也不影响它们:清理里创建的 effect 归创建它的代码
        dep(2)
        value(3)
        expect(created!.double()).toBe(6)
        expect(created!.log).toEqual([1, 2, 3])
        created!.stop()
        created!.evens.destroy()
        destroyComputed(created!.double)
        stop()
        source.destroy()
    })

    test('a write to a dependency of the re-running parent stays suppressed', () => {
        const dep = atom(0)
        let runs = 0
        const seen: number[] = []
        const stop = autorun(() => {
            runs++
            seen.push(dep())
            // 子 effect 的清理写父 effect 的依赖:父 effect 在栈顶,不会被自己这一轮触发
            autorun(({onCleanup}) => { onCleanup(() => { if (dep.raw < 10) dep(10) }) }, true)
        }, true)
        dep(1)
        expect(runs).toBe(2)
        expect(seen).toEqual([0, 10])
        dep(11)
        expect(runs).toBe(3)
        stop()
    })

    test('code after the destroy call is tracked again', () => {
        const after = atom(0)
        const go = atom(0)
        let runs = 0
        const inner = autorun(({onCleanup}) => { onCleanup(() => {}) }, true)
        const stop = autorun(() => {
            runs++
            if (go()) inner()
            after()
        }, true)
        go(1)
        after(1)
        expect(runs).toBe(3)
        stop()
    })

    test('an ambient pause survives the teardown', () => {
        const read = atom(0)
        const inner = autorun(({onCleanup}) => { onCleanup(() => {}) }, true)
        const stop = autorun(() => {
            notifier.pauseTracking()
            try {
                inner()
                expect(notifier.shouldTrack).toBe(false)
                read()
            } finally {
                notifier.resetTracking()
            }
        }, true)
        expect(subscribers(read)).toEqual([])
        stop()
    })
})

describe('tracking is restored when teardown code throws', () => {
    const boom = () => { throw new Error('teardown') }

    for (const [name, create] of Object.entries(TEARDOWNS)) {
        test(`explicit destroy / ${name}`, async () => {
            const after = atom(0)
            const go = atom(0)
            let runs = 0
            const destroy = create(boom)
            let thrown: unknown
            const stop = autorun(() => {
                runs++
                if (go() && !thrown) {
                    const depth = notifier.trackStack.length
                    try { destroy() } catch (err) { thrown = err }
                    expect(notifier.trackStack.length).toBe(depth)
                    expect(notifier.shouldTrack).toBe(true)
                }
                after()
            }, true)
            go(1)
            expect((thrown as Error).message).toBe('teardown')
            // 抛错之后的读取仍被追踪
            after(1)
            expect(runs).toBe(3)
            stop()
            await settle()
        })
    }

    test('recompute hook inside another effect', () => {
        const dep = atom(0)
        const after = atom(0)
        const go = atom(0)
        let shouldThrow = true
        let runs = 0
        const c = computed<number>(({onCleanup}) => {
            onCleanup(() => { if (shouldThrow) boom() })
            return dep()
        })
        let thrown: unknown
        const stop = autorun(() => {
            runs++
            if (go() && !thrown) {
                const depth = notifier.trackStack.length
                try { dep(1) } catch (err) { thrown = err }
                expect(notifier.trackStack.length).toBe(depth)
                expect(notifier.shouldTrack).toBe(true)
            }
            after()
        }, true)
        go(1)
        expect((thrown as Error).message).toBe('teardown')
        after(1)
        expect(runs).toBe(3)
        // 恢复探针:computed 没有冻结
        shouldThrow = false
        dep(2)
        expect(c()).toBe(2)
        stop()
        destroyComputed(c)
    })

    test('destroyChildren called directly', () => {
        const go = atom(0)
        const after = atom(0)
        let shouldThrow = true
        const parent = new LightEffect(() => {
            autorun(({onCleanup}) => { onCleanup(() => { if (shouldThrow) boom() }) }, true)
        })
        let runs = 0
        let thrown: unknown
        const stop = autorun(() => {
            runs++
            if (go() && !thrown) {
                const depth = notifier.trackStack.length
                try { parent.destroyChildren() } catch (err) { thrown = err }
                expect(notifier.trackStack.length).toBe(depth)
                expect(notifier.shouldTrack).toBe(true)
            }
            after()
        }, true)
        go(1)
        expect((thrown as Error).message).toBe('teardown')
        after(1)
        expect(runs).toBe(3)
        shouldThrow = false
        parent.destroy()
        stop()
    })

    test('children of a destroyed effect', () => {
        const go = atom(0)
        const after = atom(0)
        const parent = autorun(() => { autorun(({onCleanup}) => { onCleanup(boom) }, true) }, true)
        let runs = 0
        let thrown: unknown
        const stop = autorun(() => {
            runs++
            if (go() && !thrown) {
                const depth = notifier.trackStack.length
                try { parent() } catch (err) { thrown = err }
                expect(notifier.trackStack.length).toBe(depth)
                expect(notifier.shouldTrack).toBe(true)
            }
            after()
        }, true)
        go(1)
        expect((thrown as Error).message).toBe('teardown')
        after(1)
        expect(runs).toBe(3)
        stop()
    })
})

describe('effects created by the teardown of a re-running effect', () => {
    test('are not linked to the parent', () => {
        const dep = atom(0)
        let created: Computed | undefined
        const parent = new Computed(() => {
            if (dep() !== 0) return
            autorun(({onCleanup}) => {
                onCleanup(() => {
                    created = new Computed(() => dep())
                    created.run([], true)
                })
            }, true)
        }, undefined, true)
        parent.run([], true)
        dep(1)
        expect(created!.parent).toBe(undefined)
        expect(parent.hasChildren()).toBe(false)
        dep(2)
        parent.destroy()
        expect(created!.active).toBe(true)
        created!.destroy()
    })

    test('are not linked to the parent when they come from a deeper teardown', () => {
        const dep = atom(0)
        const created: Computed[] = []
        const create = () => {
            const effect = new Computed(() => 1)
            effect.run([], true)
            created.push(effect)
        }
        // 与父 effect 无关的结构,由子 effect 的清理销毁:它的子 effect 的清理也跑在父 effect 的帧里
        const unrelated = autorun(() => { autorun(({onCleanup}) => { onCleanup(create) }, true) }, true)
        const parent = new Computed(() => {
            if (dep() !== 0) return
            autorun(({onCleanup}) => {
                onCleanup(unrelated)
                autorun(({onCleanup: onInnerCleanup}) => { onInnerCleanup(create) }, true)
            }, true)
        }, undefined, true)
        parent.run([], true)
        dep(1)
        expect(created.length).toBe(2)
        expect(created.map(effect => effect.parent)).toEqual([undefined, undefined])
        expect(parent.hasChildren()).toBe(false)
        parent.destroy()
        expect(created.every(effect => effect.active)).toBe(true)
        created.forEach(effect => effect.destroy())
    })

    test('do not stop the parent from collecting the children of its next run, also after a throw', () => {
        const tick = atom(0)
        let childRuns = 0
        let shouldThrow = false
        const parent = new LightEffect(() => {
            autorun(({onCleanup}) => {
                tick()
                childRuns++
                onCleanup(() => { if (shouldThrow) throw new Error('teardown') })
            }, true)
        })
        parent.run()
        expect(parent.children.length).toBe(1)
        shouldThrow = true
        expect(() => parent.run()).toThrow('teardown')
        shouldThrow = false
        parent.run()
        expect(parent.children.length).toBe(1)
        expect(parent.children[0].active).toBe(true)
        const before = childRuns
        tick(1)
        expect(childRuns).toBe(before + 1)
        parent.destroy()
        tick(2)
        expect(childRuns).toBe(before + 1)
    })

    test('are not linked to the parent after a cleanup re-enters destroyChildren', () => {
        const dep = atom(0)
        let created: Computed | undefined
        const parent: LightEffect = new LightEffect(() => {
            if (dep() !== 0) return
            autorun(({onCleanup}) => {
                onCleanup(() => {
                    expect(parent.hasChildren()).toBe(false)
                    parent.destroyChildren()
                    created = new Computed(() => 1)
                    created.run([], true)
                })
            }, true)
        })
        dep(1)
        expect(created!.parent).toBe(undefined)
        expect(parent.hasChildren()).toBe(false)
        created!.destroy()
        parent.destroy()
    })

    test('a cleanup may destroy a later sibling: every child is destroyed exactly once', () => {
        const dep = atom(0)
        const cleanups: string[] = []
        const parent = new LightEffect(() => {
            if (dep() !== 0) return
            const stops: Record<string, () => void> = {}
            for (const key of ['a', 'b', 'c', 'd']) {
                stops[key] = autorun(({onCleanup}) => {
                    onCleanup(() => {
                        cleanups.push(key)
                        if (key === 'a') stops.c()
                    })
                }, true)
            }
        })
        dep(1)
        expect(cleanups).toEqual(['a', 'c', 'b', 'd'])
        expect(parent.hasChildren()).toBe(false)
        parent.destroy()
    })

    test('children after a throwing cleanup are destroyed in the same round', () => {
        const tick = atom(0)
        let phase = 0
        let secondRuns = 0
        const parent = new LightEffect(() => {
            if (phase !== 0) return
            autorun(({onCleanup}) => { onCleanup(() => { throw new Error('teardown') }) }, true)
            autorun(() => { tick(); secondRuns++ }, true)
        })
        phase = 1
        expect(() => parent.run()).toThrow('teardown')
        expect(parent.hasChildren()).toBe(false)
        tick(1)
        expect(secondRuns).toBe(1)
        expect(subscribers(tick)).toEqual([])
        parent.destroy()
    })

    test('do not remove a live sibling from the parent when they are destroyed', () => {
        const dep = atom(0)
        const tick = atom(0)
        const runs: Record<string, number> = {a: 0, b: 0, c: 0}
        let stopLate: (() => void) | undefined
        const stop = autorun(() => {
            const phase = dep()
            if (phase === 0) {
                autorun(({onCleanup}) => {
                    onCleanup(() => { stopLate = autorun(() => {}, true) })
                }, true)
            } else if (phase === 1) {
                for (const key of Object.keys(runs)) autorun(() => { tick(); runs[key]++ }, true)
            }
        }, true)
        dep(1)
        // 未修复时:late 带着上一代 children 数组的 index,按它做 swap-pop 会把 b 从新 children 里删掉
        stopLate!()
        dep(2)
        const before = {...runs}
        tick(1)
        expect(runs).toEqual(before)
        stop()
    })

    test('do not replace a child pause of the parent', () => {
        const dep = atom(0)
        let uncollected: Computed | undefined
        const parent = new Computed(() => {
            dep()
            uncollected?.destroy()
            uncollected = new Computed(() => 1)
        }, undefined, true)
        parent.run([], true)
        expect(parent.children.length).toBe(1)
        const child = parent.children[0]
        // 宿主在两轮之间暂停收集:销毁上一轮子 effect 之后必须恢复成暂停,而不是默认值
        parent.pauseCollectChild()
        dep(1)
        expect(child.active).toBe(false)
        expect(parent.shouldCollectChild).toBe(false)
        expect(parent.hasChildren()).toBe(false)
        uncollected!.destroy()
        parent.destroy()
    })

    test('a child cleanup that destroys its parent with ignoreChildren during destroyChildren', () => {
        // destroyChildren 期间 _children 指向冻结的哨兵:ignoreChildren 分支不能去清空它
        // (TypeError),正在进行的这一轮销毁照常把剩下的子 effect 销毁完。
        const cleaned: string[] = []
        const parent: Computed = new Computed(() => {
            autorun(({onCleanup}) => { onCleanup(() => { cleaned.push('first'); parent.destroy(true) }) }, true)
            autorun(({onCleanup}) => { onCleanup(() => { cleaned.push('second') }) }, true)
        }, undefined, true)
        parent.run([], true)
        expect(() => parent.destroyChildren()).not.toThrow()
        expect(cleaned).toEqual(['first', 'second'])
        expect(parent.active).toBe(false)
        expect(parent.hasChildren()).toBe(false)
    })
})

/**
 * 差分 fuzz:清理里的追踪读取 ≡ .raw 读取。
 * 同一份随机 effect 森林在两个世界里执行同一串操作,唯一区别是清理钩子读 atom 的方式;
 * 两个世界的执行日志(谁在什么时候跑、清理顺序)与最终订阅关系必须逐项相等。
 */
describe('differential fuzz: tracked reads in teardown ≡ raw reads', () => {
    type Node = {
        id: number
        kind: 'autorun' | 'computed' | 'light' | 'row'
        reads: number[]
        cleanupReads: number[]
        destroyReads: number[]
        writes?: number
        children: Node[]
    }
    const SOURCES = 5
    const CASCADES = 3

    function randomForest(rand: () => number) {
        let nextId = 0
        const pick = (max: number, count: number) =>
            Array.from({length: Math.floor(rand() * (count + 1))}, () => Math.floor(rand() * max))
        const node = (depth: number, isRoot: boolean): Node => {
            // 只有根节点写 cascade atom,且写者只读 source atom:依赖图无环,两个世界都必然收敛
            const writes = isRoot && rand() < 0.35 ? SOURCES + Math.floor(rand() * CASCADES) : undefined
            const readable = writes === undefined ? SOURCES + CASCADES : SOURCES
            return {
                id: nextId++,
                kind: (['autorun', 'computed', 'light', 'row'] as const)[Math.floor(rand() * 4)],
                reads: pick(readable, 3),
                cleanupReads: pick(SOURCES + CASCADES, 3),
                destroyReads: pick(SOURCES + CASCADES, 2),
                writes,
                children: depth === 0 || writes !== undefined ? [] : Array.from({length: Math.floor(rand() * 3)}, () => node(depth - 1, false)),
            }
        }
        return Array.from({length: 3 + Math.floor(rand() * 4)}, () => node(2, true))
    }

    function runWorld(seed: number, tracked: boolean) {
        const rand = mulberry32(seed)
        const forest = randomForest(rand)
        const atoms = Array.from({length: SOURCES + CASCADES}, () => atom(0))
        const ids = new Map<ReactiveEffect, number>()
        const log: string[] = []
        const read = (index: number) => { tracked ? atoms[index]() : atoms[index].raw }

        function mount(node: Node): () => void {
            const body = (onCleanup?: (fn: () => void) => void) => {
                log.push(`run ${node.id}`)
                let sum = 0
                // 动态依赖:读到偶数时少读一个
                for (const index of node.reads) {
                    const value = atoms[index]()
                    sum += value
                    if (value % 2 === 0 && node.reads.length > 2) break
                }
                onCleanup?.(() => {
                    log.push(`cleanup ${node.id}`)
                    node.cleanupReads.forEach(read)
                })
                node.children.forEach(mount)
                if (node.writes !== undefined) atoms[node.writes](sum)
                return sum
            }
            const onDestroy = () => {
                log.push(`destroy ${node.id}`)
                node.destroyReads.forEach(read)
            }
            switch (node.kind) {
                case 'autorun': {
                    const stop = autorun(({onCleanup}) => { body(onCleanup) }, true)
                    return stop
                }
                case 'computed': {
                    const c = computed<number>(({onCleanup}) => body(onCleanup), undefined, undefined, {onDestroy})
                    return () => destroyComputed(c)
                }
                case 'light': {
                    const effect = new LightEffect(() => body())
                    ids.set(effect, node.id)
                    effect.on('destroy', onDestroy)
                    return () => effect.destroy()
                }
                case 'row': {
                    const list = new RxList([0])
                    const mapped = list.map((_item, _index, context) => body(context.onCleanup))
                    mapped.on('destroy', onDestroy)
                    return () => mapped.destroy()
                }
            }
        }

        const stops = forest.map(mount)
        for (let step = 0; step < 25; step++) {
            log.push(`-- step ${step}`)
            const roll = rand()
            const write = () => atoms[Math.floor(rand() * SOURCES)](Math.floor(rand() * 5))
            if (roll < 0.65) write()
            else if (roll < 0.85) batch(() => { write(); write() })
            else {
                const index = Math.floor(rand() * stops.length)
                // 在别的 effect 执行中销毁
                const stop = autorun(() => { stops[index]() }, true)
                log.push(`outer subscriptions ${atoms.filter(a => subscribers(a).length).length}`)
                stop()
            }
            log.push(`subscribers ${atoms.map(a => subscribers(a).length).join(',')}`)
        }
        stops.forEach(stop => stop())
        log.push(`left ${atoms.map(a => subscribers(a).length).join(',')}`)
        return log
    }

    test('60 seeds', () => {
        let cleanups = 0
        for (let seed = 1; seed <= 60; seed++) {
            const raw = runWorld(seed, false)
            const tracked = runWorld(seed, true)
            cleanups += raw.filter(line => line.startsWith('cleanup')).length
            const diff = raw.findIndex((line, i) => line !== tracked[i])
            expect(
                diff === -1 ? tracked.length : tracked.slice(Math.max(0, diff - 6), diff + 3),
                `seed ${seed} diverges at entry ${diff}: expected ${raw[diff]}`,
            ).toEqual(diff === -1 ? raw.length : raw.slice(Math.max(0, diff - 6), diff + 3))
            // 全部销毁后不留订阅
            expect(raw[raw.length - 1]).toBe(`left ${new Array(SOURCES + CASCADES).fill(0).join(',')}`)
        }
        // 生成器自检:清理确实在执行
        expect(cleanups).toBeGreaterThan(500)
    })
})

/**
 * 销毁快路径:没有子 effect、没有监听者、没有子类资源钩子的 effect 销毁时不会执行用户代码,
 * 静态 destroy 直接摘订阅、离开父 effect。下面钉住两件事:快路径做的事和完整路径一样;
 * 只要三个条件有一个不成立,就走完整路径(子 effect 被销毁、监听者被通知、资源钩子被执行)。
 */
describe('destroy fast path for effects that cannot run user code', () => {
    test('a plain effect destroyed on its own is unsubscribed and leaves its parent', () => {
        const dep = atom(0)
        const tick = atom(0)
        let plain!: LightEffect
        let plainRuns = 0
        let siblingCleanups = 0
        const parent = new Computed(() => {
            dep()
            plain = new LightEffect(() => { tick(); plainRuns++ })
            autorun(({onCleanup}) => { onCleanup(() => { siblingCleanups++ }) }, true)
        }, undefined, true)
        parent.run([], true)
        expect(parent.children.length).toBe(2)
        expect(subscribers(tick)).toEqual([plain])

        plain.destroy()
        expect(plain.active).toBe(false)
        expect(plain.parent).toBeUndefined()
        expect(parent.children.length).toBe(1)
        expect(subscribers(tick)).toEqual([])
        tick(1)
        expect(plainRuns).toBe(1)
        // 幂等
        plain.destroy()
        expect(parent.children.length).toBe(1)
        // 父 effect 的记账没被弄乱:重算只清理还活着的兄弟
        dep(1)
        expect(siblingCleanups).toBe(1)
        parent.destroy()
    })

    test('a plain child is destroyed with its parent', () => {
        const tick = atom(0)
        let runs = 0
        const stop = autorun(() => { new LightEffect(() => { tick(); runs++ }) }, true)
        expect(subscribers(tick)).toHaveLength(1)
        stop()
        expect(subscribers(tick)).toEqual([])
        tick(1)
        expect(runs).toBe(1)
    })

    test('with a child effect: the child is destroyed', () => {
        const tick = atom(0)
        let childRuns = 0
        const host = new LightEffect(() => { new LightEffect(() => { tick(); childRuns++ }) })
        expect(host.hasChildren()).toBe(true)
        host.destroy()
        expect(subscribers(tick)).toEqual([])
        tick(1)
        expect(childRuns).toBe(1)
    })

    test('with a destroy listener: the listener is notified', () => {
        const dep = atom(0)
        const e = new LightEffect(() => { dep() })
        let notified = 0
        e.on('destroy', () => { notified++ })
        e.destroy()
        expect(notified).toBe(1)
        expect(subscribers(dep)).toEqual([])
    })

    test('with a listener for another event only: destroy still completes', () => {
        const dep = atom(0)
        const e = new LightEffect(() => { dep() })
        e.on('custom', () => {})
        e.destroy()
        expect(e.active).toBe(false)
        expect(subscribers(dep)).toEqual([])
    })

    test('with a subclass resource hook: the hook runs, and its reads are isolated', () => {
        const read = atom(0)
        const go = atom(0)
        let released = 0
        class WithResource extends LightEffect {
            destroyResources() {
                released++
                read()
            }
        }
        const e = new WithResource(() => {})
        let outerRuns = 0
        const stop = autorun(() => {
            outerRuns++
            if (go()) e.destroy()
        }, true)
        go(1)
        expect(released).toBe(1)
        expect(subscribers(read)).toEqual([])
        read(1)
        expect(outerRuns).toBe(2)
        stop()
    })

    // 销毁协议里其余可覆写的方法（review 发现）：2.16.0 每次销毁都以方法调用执行 destroyChildren()、
    // cleanup()、dispatch('destroy')，覆写它们的子类不能因为快路径而被跳过。
    test('with a destroyChildren() override: it is still called on destroy and on re-run', () => {
        const dep = atom(0)
        const seen: string[] = []
        class Owning extends LightEffect {
            destroyChildren() {
                seen.push('destroyChildren')
                super.destroyChildren()
            }
        }
        const e = new Owning(() => { dep() })
        seen.length = 0
        dep(1)
        expect(seen).toEqual(['destroyChildren'])
        e.destroy()
        expect(seen).toEqual(['destroyChildren', 'destroyChildren'])
        expect(subscribers(dep)).toEqual([])
    })

    test('with a dispatch() override: it still sees the destroy event', () => {
        const seen: string[] = []
        class Forwarding extends LightEffect {
            dispatch(event: string, arg?: any) {
                seen.push(event)
                super.dispatch(event, arg)
            }
        }
        const e = new Forwarding(() => {})
        let notified = 0
        e.on('destroy', () => { notified++ })
        seen.length = 0
        e.destroy()
        expect(seen).toEqual(['destroy'])
        expect(notified).toBe(1)
        const bare = new Forwarding(() => {})
        seen.length = 0
        bare.destroy()
        expect(seen).toEqual(['destroy'])
    })

    test('with a cleanup() override: the reads in it are isolated', () => {
        const read = atom(0)
        const go = atom(0)
        class Reading extends LightEffect {
            cleanup() {
                read()
                super.cleanup()
            }
        }
        const e = new Reading(() => {})
        let outerRuns = 0
        const stop = autorun(() => {
            outerRuns++
            if (go()) e.destroy()
        }, true)
        go(1)
        expect(e.active).toBe(false)
        read(1)
        expect(outerRuns).toBe(2)
        stop()
    })
})
