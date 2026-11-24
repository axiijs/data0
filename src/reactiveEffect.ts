import {Dep, finalizeDepMarkers, initDepMarkers, pruneEmptyDepFromHost, restoreEffectDeps, retainDepMarkers} from "./dep.js";
import {maxMarkerBits, Notifier, notifier} from "./notify.js";
import type {InputTriggerInfo, TriggerInfo} from "./notify.js";
import type {TriggerOpTypes} from "./operations.js";
import {ManualCleanup} from "./manualCleanup.js";
import {assert, isAsync, isGenerator} from "./util.js";
import {
    trackRetainedDepEffectAdded,
    trackRetainedDepEffectRemoved,
    trackRetainedReactiveEffectCreated,
    trackRetainedReactiveEffectDestroyed
} from "./retainedDiagnostics";


// CAUTION 共享的"空 deps"哨兵：所有 effect 初始都指向它，第一次 track 到 dep 时
//  才替换成容量恰好为 1 的 [dep]（见 addDep）。两个动机：
//  1. 渲染框架里绝大多数绑定 effect 只有 0 或 1 个 dep。旧实现 `deps = []` 后第一次
//     push 会让 V8 直接把 elements store 扩到容量 17（0 + 0>>1 + 16），每个单 dep
//     effect 白付 64B 常驻内存；`[dep]` 字面量的容量恰好是 1。
//  2. 从未 track 到依赖的 effect（静态内容的 FunctionHost、探测后无依赖的 map 行）
//     完全不为 deps 分配数组。
//  所有写入必须走 addDep/transferCapturesTo；读取方（cleanup/dep markers）
//  都有 length 守卫，对共享空数组只读不写。freeze 保证未来出现绕过 addDep 的
//  直接 push 时立刻抛错（ESM 严格模式），而不是静默地跨 effect 污染订阅。
const SHARED_EMPTY_DEPS: Dep[] = Object.freeze([]) as unknown as Dep[]

// CAUTION "正在销毁上一轮 children"的哨兵：destroyChildren 执行期间 effect._children 指向它
//  （见该方法说明）。用既有字段的值而不是新增实例字段表达，effect 的 hidden class 不变；
//  freeze 保证绕过构造器守卫的直接 push 立刻抛错。
const CHILDREN_IN_TEARDOWN: ReactiveEffect[] = Object.freeze([]) as unknown as ReactiveEffect[]

/**
 * @internal
 * 清理隔离原语：库在重算或销毁中调用用户清理前使用，返回 true 时调用方必须在 finally 里
 * `notifier.resetTracking()`。
 *
 * CAUTION 不变量：清理代码的读取不属于任何 effect。清理跑在「恰好正在执行的 effect」的
 *  帧里——父 effect 重算销毁子 effect、computed 在别的 effect 执行中被重算、effect 执行中
 *  显式 stop/destroy——那个 effect 若追踪了清理里的读取，之后写那个 atom 就会重跑它。
 *  暂停追踪而不是改动 activeScopes：栈顶抑制自触发的语义不变；清理触发或创建的 effect
 *  在自己的 run 里 enableTracking，照常收集依赖。
 *  没有正在执行的 effect、或追踪已经暂停时没有可隔离的对象，不付开关成本。
 *  行为执法见 __tests__/teardownTracking.spec.ts。
 */
export function isolateTeardown(): boolean {
    if (!notifier.shouldTrack || ReactiveEffect.activeScopes.length === 0) return false
    notifier.pauseTracking()
    return true
}

/**
 * @internal
 * 销毁收尾中、第一个错误之后再出现的错误：第一个错误会抛给调用方，这些只能上报。
 * 子类的 destroyResources 钩子（Computed / RxList）沿用同一规则，所以导出给它们用。
 */
export function reportTeardownError(error: unknown) {
    console.error('[data0] uncaught error in teardown:', error)
}

// 被销毁或被取代的 generator（getter 或 applyPatch）不再续跑：库停在它挂起的 yield 处。
// 它自己的 finally 是它的清理代码（释放锁、关闭连接……），照 JS 放弃迭代器的约定用 return()
// 关闭它：yield 之后的函数体不再执行，finally 照常执行。finally 是清理代码：读取不追踪，
// 抛错没有同步调用方，只上报。
function closeAbandonedGenerator(generator: Generator<any, any, any>) {
    const isolated = isolateTeardown()
    try {
        generator.return(undefined)
    } catch (error) {
        reportTeardownError(error)
    } finally {
        if (isolated) notifier.resetTracking()
    }
}

// CAUTION 子 effect 的 destroy() 覆写在走到静态核心之前抛错：它仍是 active、仍订阅着依赖、
//  还留着 parent 与这一代 children 的 index，而这一代数组马上要被 destroyChildren 丢弃——它就成了
//  活着的孤儿（没有人会再销毁它；之后被单独销毁时还会按陈旧 index 在新 children 上 swap-pop，删掉
//  活着的兄弟）。绕过已经抛过错的覆写，用静态核心把它销毁完；fromParent：父 effect 自己清空
//  children。覆写在 super.destroy() 之后才抛错时它已经 inactive，这里什么都不做。
function finishChildAfterError(child: ReactiveEffect | undefined) {
    if (child === undefined || !child.active) return
    try {
        ReactiveEffect.destroy(child, true)
    } catch (error) {
        reportTeardownError(error)
    }
}

export class ReactiveEffect extends ManualCleanup {
    static activeScopes: ReactiveEffect[] = []
    public active: boolean
    // CAUTION isRunningAsync/useDepMarker/index/shouldCollectChild 的默认值放在原型上
    //  （见 class 定义后的赋值），实例只在真正改写时才产生自有属性。
    //  渲染框架里每个绑定都是一个 effect，这些"恒定默认值"的实例槽位在长列表下是可观的常驻内存。
    declare public isRunningAsync: boolean
    // 栈内销毁的延迟标记（见 static destroy 的 CAUTION）。位掩码：
    // 1=pending, 2=fromParent, 4=ignoreChildren——flush 时按原调用参数重放
    // （fromParent 尤其重要：destroyChildren 发起的延迟销毁若按 false 重放，
    // 会拿陈旧 index 在父亲重建后的 children 数组上做 swap-pop，删错兄弟）。
    // 默认值在原型上，只有"run 栈内被 destroy"的错误边界路径才写实例位。
    declare public _pendingDestroy: number
    private _eventToCallbacks?: Map<string, Set<Function>>
    private _asyncTracks?: Array<() => void>
    private _children?: ReactiveEffect[]
    static destroy(effect: ReactiveEffect, fromParent = false, ignoreChildren = false) {
        if (!effect.active) return
        // CAUTION 正在执行中的 effect（在 activeScopes 上）不能就地拆除：
        //  立刻 cleanup() 会把本轮 run 已置位的 dep marker（dep.n/w 的本深度位）留成
        //  脏位——deps 已清空，completeTracking 的 finalizeDepMarkers 无从复位，之后
        //  同一深度 track 同一 dep 的其他 effect 会被 newTracked 误判为已订阅而
        //  **静默漏订阅**（once() 注释里记载的同一缺陷类）。v2.7.0 曾用断言直接禁止
        //  "栈内销毁"，但同步重入整树销毁是下游错误边界的既定写法（axle 的 error
        //  钩子 → root.destroy() → destroyComputed 正在 patch 的 computed，其
        //  doc/02 §4；data0 <= 2.6 一直允许），断言把该模式全部打崩。
        //  两全方案：延迟销毁——置 _pendingDestroy 并立即返回，completeTracking
        //  在栈展开、marker 复位之后按原参数重放真正的销毁。对调用方语义仍是
        //  "销毁已生效"：flush 先于本段同步栈之后的一切外部代码执行。
        //  回归钉在 __tests__/verifiedReviewFixes.spec.ts 的 destroy-inside-run 组。
        if (ReactiveEffect.activeScopes.includes(effect)) {
            // CAUTION 累加而不是覆盖：执行期间可能被销毁不止一次，两个参数位一旦置上都不可撤回。
            //  - fromParent：父 effect 重算时 destroyChildren 已经把本 effect 从它的 children 里
            //    放掉、并建好了下一代 children；之后再来一次普通的 destroy()（旧 child 的句柄被
            //    多停了一次、child 在自己的执行里自停）若把这一位冲掉，flush 就按 false 重放——
            //    拿上一代的 index 在新一代数组上 swap-pop，把新一代的兄弟挤出 children，
            //    它从此不随父亲重算 / 销毁（正是上面 _pendingDestroy 说明里要避免的事）。
            //  - ignoreChildren：某个调用方声明了"children 由我接管"，后来的普通 destroy()
            //    不能再替它把这些 children 销毁掉。
            effect._pendingDestroy |= 1 | (fromParent ? 2 : 0) | (ignoreChildren ? 4 : 0)
            return
        }

        // CAUTION 先置 inactive 再执行清理：destroyResources 会运行用户 cleanup，
        //  其中的响应式写入若再触发本 effect（或重入 destroy），都以 active === false
        //  被安全拦截，不会出现"销毁中重算/重复销毁"。
        effect.active = false
        trackRetainedReactiveEffectDestroyed(effect)
        // CAUTION 快路径：没有子 effect、没有任何监听者、也没有子类资源钩子的 effect——渲染框架里
        //  的属性 / 文本绑定几乎都是这样——销毁时不会执行任何用户代码：不需要隔离，不可能抛错，
        //  也没有 destroy 事件要派发。摘掉订阅、离开父 effect 就结束。这是列表卸载时每个绑定都走的路径。
        //  销毁协议里可覆写的方法（cleanup / destroyChildren / dispatch）被子类覆写时，覆写就是用户代码，
        //  走下面的完整路径：照 2.16.0 的协议调用它们，并且隔离其中的读取。
        if (effect._children === undefined && effect._eventToCallbacks === undefined && effect.destroyResources === noDestroyResources
            && effect.cleanup === baseCleanup && effect.destroyChildren === baseDestroyChildren && effect.dispatch === baseDispatch) {
            effect.cleanup()
            ReactiveEffect.unlinkFromParent(effect, fromParent)
            return
        }
        // 以下三处都会执行用户清理代码：destroyResources、子 effect 的销毁、destroy 事件
        const isolated = isolateTeardown()
        let reachedDestroyEvent = false
        try {
            // CAUTION 子类资源清理钩子（惰性 meta、lastCleanupFn、cleanPromise、行级
            //  effect frame 等）。所有销毁入口——实例 destroy()、destroyChildren、
            //  destroyComputed——都汇聚到本静态核心，钩子恰好执行一次。旧实现把这些
            //  清理放在各子类的 destroy() 覆写里，destroyChildren/destroyComputed 走
            //  静态函数时全部被绕过（子 computed 的 onCleanup 从不执行、惰性 meta 泄漏）。
            effect.destroyResources()
            effect.cleanup()

            ReactiveEffect.unlinkFromParent(effect, fromParent)
            if (!ignoreChildren) {
                effect.destroyChildren()
            } else if (effect._children && effect._children !== CHILDREN_IN_TEARDOWN) {
                // CAUTION ignoreChildren 语义是"留下 children 由调用方自行管理"，
                //  但必须断开它们的 parent 反向指针：否则这些孤儿 child 之后被单独
                //  destroy 时，会在已清空的 siblings 数组上做 last 替换（pop 出
                //  undefined 后写 last.index 直接 TypeError）。
                const children = effect._children
                for (let i = 0; i < children.length; i++) {
                    if (children[i].parent === effect) children[i].parent = undefined
                }
                children.length = 0
            }
            reachedDestroyEvent = true
            ReactiveEffect.dispatchDestroy(effect)
        } catch (error) {
            // CAUTION 用户清理抛错不能让销毁停在一半：active 已经是 false，之后任何销毁入口都会
            //  跳过本 effect——没摘掉的订阅、没销毁的子 effect 就永远留下了。把剩下的步骤做完，
            //  再把第一个错误抛给调用方；收尾中再出的错误只上报。
            if (!reachedDestroyEvent) ReactiveEffect.finishDestroyAfterError(effect, fromParent, ignoreChildren)
            throw error
        } finally {
            if (isolated) notifier.resetTracking()
        }
    }
    // 离开父 effect。幂等：以 parent 为准，做过一次之后再调用什么都不做。
    private static unlinkFromParent(effect: ReactiveEffect, fromParent: boolean) {
        const parent = effect.parent
        if (parent === undefined) return
        // 如果不是 fromParent，就要从父亲中移除。如果是，父亲会自己清空 children
        if (!fromParent) {
            // 要把自己从 parent.children 中移除掉。直接用 last 替换掉当前的要上出的，提升删除速度。
            const siblings = parent._children
            // CAUTION length 守卫：防御 siblings 已被外部清空时 pop 出 undefined
            if (siblings && siblings.length) {
                const last = siblings.pop()!
                if (last !== effect) {
                    siblings[effect.index!] = last
                    last.index = effect.index
                }
            }
        }
        // CAUTION 赋值 undefined 而不是 delete：delete 会把对象推进字典属性模式
        effect.parent = undefined
    }
    // 销毁中途用户清理抛错后的慢路径：把上面没做完的步骤做完。与正常路径同序，每一步都幂等
    // （cleanup 以 deps.length 为准，断父链以 parent 为准，destroyChildren 抛错前已把所有
    // 子 effect 销毁完），所以不需要知道是哪一步抛的错。正常路径保持内联，不为这里付调用成本。
    private static finishDestroyAfterError(effect: ReactiveEffect, fromParent: boolean, ignoreChildren: boolean) {
        effect.cleanup()
        ReactiveEffect.unlinkFromParent(effect, fromParent)
        try {
            if (!ignoreChildren) {
                effect.destroyChildren()
            } else if (effect._children && effect._children !== CHILDREN_IN_TEARDOWN) {
                const children = effect._children
                for (let i = 0; i < children.length; i++) {
                    if (children[i].parent === effect) children[i].parent = undefined
                }
                children.length = 0
            }
        } catch (error) {
            reportTeardownError(error)
        }
        try {
            ReactiveEffect.dispatchDestroy(effect)
        } catch (error) {
            reportTeardownError(error)
        }
    }
    // destroy 监听者是清理代码（onDestroy 回调、派生结构挂的级联销毁）：一个抛错不能让排在后面的
    // 收不到通知——它们要释放的东西就永久留下了。全部通知完再抛出第一个错误，其余的上报。
    // 绝大多数 effect 没有监听者，直接返回。
    // 覆写了 dispatch 的子类照旧经 dispatch('destroy') 收到事件（2.16.0 的协议），监听者之间的
    // 错误隔离由覆写自己负责。
    private static dispatchDestroy(effect: ReactiveEffect) {
        if (effect.dispatch !== baseDispatch) {
            effect.dispatch('destroy')
            return
        }
        const callbacks = effect._eventToCallbacks?.get('destroy')
        if (callbacks === undefined) return
        let failed = false
        let first: unknown
        callbacks.forEach(callback => {
            try {
                callback.call(effect)
            } catch (error) {
                if (failed) {
                    reportTeardownError(error)
                } else {
                    failed = true
                    first = error
                }
            }
        })
        if (failed) throw first
    }

    deps: Dep[] = SHARED_EMPTY_DEPS
    // CAUTION deps 的唯一合法写入口（见 SHARED_EMPTY_DEPS 的说明）
    addDep(dep: Dep) {
        const deps = this.deps
        if (deps === SHARED_EMPTY_DEPS) {
            this.deps = [dep]
        } else {
            deps.push(dep)
        }
    }
    // 有增量计算的情况会 manual track dep，这时不要做 dep marker，因为不需要 finalize 自动对比的计算的过程。
    declare useDepMarker: boolean
    parent?: ReactiveEffect
    declare index: number
    declare getter?: (...args: any[]) => any
    isAsync?:boolean
    declare shouldCollectChild: boolean
    // CAUTION 触发协议（见 notify.ts triggerEffect）：
    //  needsTriggerInfo 表示该 effect 会消费 TriggerInfo（patch 型 Computed / 声明了
    //  第三个参数的自定义调度器）。为 false 时 trigger 路径完全不构造 info 对象——
    //  渲染框架的轻量绑定 effect 都不读 info，这是 trigger 热路径零分配的关键。
    //  默认值在原型上（Computed 构造器在需要时置 true）。
    declare needsTriggerInfo: boolean
    // effect session（batch）内的去重标记与待处理 info 队列，代替原来 notifier 上的
    // Set + WeakMap（每次 batch 内触发省一次哈希查找与集合分配）。默认值在原型上。
    declare _inSession: boolean
    declare _sessionInfos?: TriggerInfo[]
    constructor(getter?: (...args: any[]) => any) {
        // 这是为了支持有的数据结构想写成 source/computed 都支持的情况，比如 RxList。它会继承 Computed
        super();
        // CAUTION getter/isAsync 只在有 getter 时才写实例属性：
        //  轻量绑定 effect（无 getter）不为用不到的字段付槽位
        if (getter !== undefined) {
            this.getter = getter
            this.isAsync = isAsync(getter) || isGenerator(getter)
        }
        this.active = !!getter
        if (this.active) trackRetainedReactiveEffectCreated(this)

        const scopes = ReactiveEffect.activeScopes
        if (scopes.length) {
            const parent = scopes[scopes.length - 1]
            if (parent.shouldCollectChild && parent._children !== CHILDREN_IN_TEARDOWN) {
                this.parent = parent
                this.index = parent.addChild(this)
            }
        }
    }
    /**
     * 在"游离"上下文中创建响应式对象：临时屏蔽父 effect 的 children 收集与
     * ManualCleanup 的 frame 收集。用于惰性创建生命周期由宿主自己管理的派生结构
     * （RxList.length、RxMap.keys 等 meta）——否则在 autorun/computed 的 getter 里
     * 首次访问这些 meta 时，它们会被当作该 effect 的 child，在下一次重算的
     * cleanup 中被误销毁。
     */
    static createDetached<T>(create: () => T): T {
        const scopes = ReactiveEffect.activeScopes
        const top = scopes.length ? scopes[scopes.length - 1] : undefined
        const prevShouldCollect = top ? top.shouldCollectChild : undefined
        if (top) top.shouldCollectChild = false
        // 压入一个丢弃 frame，创建过程中的 ManualCleanup 实例不会进入外层 frame
        const stopCollect = ManualCleanup.collectEffect()
        try {
            return create()
        } finally {
            stopCollect()
            if (top) top.shouldCollectChild = prevShouldCollect!
        }
    }
    get eventToCallbacks() {
        return this._eventToCallbacks ?? (this._eventToCallbacks = new Map())
    }
    get asyncTracks() {
        return this._asyncTracks ?? (this._asyncTracks = [])
    }
    get children() {
        return this._children ?? (this._children = [])
    }
    hasChildren() {
        return !!this._children?.length
    }
    addChild(child: ReactiveEffect) {
        const children = this._children ?? (this._children = [])
        children.push(child)
        return children.length - 1
    }
    destroyChildren() {
        const children = this._children
        if (!children || children === CHILDREN_IN_TEARDOWN) return
        if (children.length) {
            // CAUTION prepareTracking 在本 effect 入栈之后、enableTracking 之前调用这里，
            //  子 effect 的清理（含 destroy 覆写）因此跑在本 effect 的帧里：
            //  1. 读取不追踪（见 isolateTeardown；逐个子 effect 的静态 destroy 也会隔离，这里
            //     覆盖 destroy 覆写里的用户代码，并让逐个销毁不再重复开关）；
            //  2. 清理里创建的 effect 不收集：曾被追加进正在销毁的数组、随数组一起丢弃，
            //     却留着 parent 与这一代数组的 index——之后单独 destroy 时按陈旧 index
            //     在新 children 上做 swap-pop，删掉无关的兄弟（兄弟从此不随宿主重算
            //     销毁）。生命周期不变：此前同样不会被本 effect 销毁。
            //  3. 一个子 effect 的清理抛错，不能让它后面的兄弟留下：这可能是最后一次销毁
            //     （本 effect 自己正在被销毁），不会再有下一轮来收拾。把剩下的销毁完，
            //     再抛出第一个错误；其余的错误只上报。
            this._children = CHILDREN_IN_TEARDOWN
            const isolated = isolateTeardown()
            let i = 0
            try {
                for (; i < children.length; i++) {
                    // CAUTION 走实例方法（fromParent=true）：destroy 可能被子类覆写，
                    //  静态调用会绕过覆写与 destroyResources 钩子链。
                    //  ?.：数组可能有空洞（历史上按陈旧 index 做 swap-pop 留下的）。
                    children[i]?.destroy(false, true)
                }
            } catch (error) {
                finishChildAfterError(children[i])
                for (i++; i < children.length; i++) {
                    try {
                        children[i]?.destroy(false, true)
                    } catch (laterError) {
                        reportTeardownError(laterError)
                        finishChildAfterError(children[i])
                    }
                }
                throw error
            } finally {
                if (isolated) notifier.resetTracking()
                this._children = undefined
            }
            return
        }
        this._children = undefined
    }
    queueAsyncTrack(track: () => void) {
        (this._asyncTracks ?? (this._asyncTracks = [])).push(track)
    }
    // CAUTION 原型方法而不是实例箭头函数：每个 effect 实例少 3 个闭包的常驻内存
    //  （渲染框架里每个绑定就是一个 effect，长列表下这是可观的量）。
    //  需要脱离 this 使用的地方（Computed.createGetterContext）自行 bind。
    pauseCollectChild() {
        this.shouldCollectChild = false
    }
    resumeCollectChild() {
        this.shouldCollectChild = true
    }

    on(event: string, callback: Function) {
        let callbacks = this._eventToCallbacks?.get(event)
        if (!callbacks) {
            callbacks = new Set()
            ;(this._eventToCallbacks ?? (this._eventToCallbacks = new Map())).set(event, callbacks)
        }
        callbacks.add(callback)
    }
    off(event: string, callback: Function) {
        let callbacks = this._eventToCallbacks?.get(event)
        if (callbacks) {
            callbacks.delete(callback)
        }
    }
    // CAUTION 热路径守卫：track/trigger 每次都会经过事件派发点，绝大多数 effect 没有监听者。
    //  调用方先用它判断，再构造 payload，避免无监听者时的对象分配。
    hasListener(event: string) {
        const callbacks = this._eventToCallbacks?.get(event)
        return callbacks !== undefined && callbacks.size > 0
    }
    // CAUTION 单参签名而不是 ...args：内部所有派发点最多只带一个参数，
    //  rest 参数会让每次 dispatch 都分配一个数组（trigger/track/recompute 等热路径每次触发都要派发）。
    dispatch(event: string, arg?: any) {
        const callbacks = this._eventToCallbacks?.get(event)
        if (callbacks) {
            callbacks.forEach(callback => callback.call(this, arg))
        }
    }
    createGetterContext():any {
        return undefined
    }
    callGetter():any {

    }

    // CAUTION 原子性：要么完成，要么不留痕迹。销毁上一轮的子 effect 会执行用户清理，
    //  清理抛错时这里撤销已做的一切（出栈、深度与 marker 位复原、保留上一轮的依赖、
    //  执行期间登记的延迟销毁照常生效）再抛出。调用方因此可以把它放在 try 之外——
    //  callSimpleGetter/callAsyncGetter 一直如此，若不撤销，宿主永久留在 activeScopes
    //  上，之后任何顶层读取都会订阅它。
    prepareTracking(isFirst = false, isAsync = this.isAsync) {
        let previousDeps: Dep[] | undefined
        if (!isAsync) {
            Notifier.trackOpBit = 1 << ++notifier.effectTrackDepth
            ReactiveEffect.activeScopes.push(this)

            if (this.useDepMarker && notifier.effectTrackDepth <= maxMarkerBits) {
                initDepMarkers(this)
            } else {
                // full cleanup 模式在销毁子 effect 之前就清空依赖。有子 effect 时留一份快照：
                // 子 effect 的清理抛错的话，下面用它恢复，否则本 effect 从此不再响应。
                if (this._children !== undefined && this.deps.length) previousDeps = this.deps.slice()
                this.cleanup()
            }
        } else {
            // async 模式下是通过暂存一个 track 函数到 asyncTracks 中，然后在 completeTracking 时执行。
            // 所以这里只需要 push scope 就行了。
            ReactiveEffect.activeScopes.push(this)
            if (!isFirst) return
            if (this._asyncTracks) this._asyncTracks.length = 0
        }

        // 没有子 effect 就没有清理要跑、不会抛错：最热的路径（绑定类 effect 的重跑）不进 try。
        //  覆写了 destroyChildren 的子类照 2.16.0 的协议每轮都调用它。
        if (this._children === undefined && this.destroyChildren === baseDestroyChildren) return
        try {
            this.destroyChildren()
        } catch (err) {
            // marker 模式下把上一轮的依赖全部视为本轮已收集，finalizeDepMarkers 保留它们并
            // 复位位标记。
            // CAUTION full cleanup 模式不能标记：依赖已在上面清空，清理里读到的 atom 此刻可能已被
            //  追踪成新依赖，而该模式下 completeTracking 不做 finalize，位标记会留在 dep 上——
            //  之后同一深度读这个 atom 的 effect 会被判为已订阅而静默漏订阅。该模式用上面的
            //  快照恢复依赖（Computed 的错误恢复随后还会 restoreEffectDeps 一次，结果相同）。
            if (!isAsync && this.useDepMarker && notifier.effectTrackDepth <= maxMarkerBits) {
                retainDepMarkers(this)
            } else if (previousDeps !== undefined) {
                restoreEffectDeps(this, previousDeps)
            }
            // 清理里登记的延迟销毁（例如子 effect 的清理销毁了正在重算的本 effect）在这里执行，
            // 它抛出的是第二个错误：第一个错误抛给调用方，这个只上报。completeTracking 先出栈、
            // 复原深度再执行延迟销毁，抛错时栈已经复原。
            try {
                this.completeTracking(false, isAsync)
            } catch (destroyError) {
                reportTeardownError(destroyError)
            }
            throw err
        }
    }

    /**
     * CAUTION 按身份出栈，绝不盲目 pop：这是作用域栈唯一合法的移除入口。
     *  同步执行严格嵌套（自己必然在栈顶，只多一次比较）；但 async 执行
     *  （async patch/generator）可能交错完成，栈顶可能是别人——盲目 pop 会把
     *  对方弹掉，之后对方的一切 track 归属到错误的 effect 上（订阅污染）。
     *  该原语从构造上消灭"弹错栈"这一类缺陷。
     */
    private removeFromActiveScopes() {
        const scopes = ReactiveEffect.activeScopes
        if (scopes[scopes.length - 1] === this) {
            scopes.pop()
            return
        }
        const index = scopes.lastIndexOf(this)
        if (index !== -1) scopes.splice(index, 1)
    }

    completeTracking(isLast = false, isAsync = this.isAsync) {
        if (!isAsync) {
            // CAUTION dev 不变量：同步的 completeTracking 必须与一次仍在栈上的 prepareTracking 配对。
            //  prepareTracking 失败时自己撤销（见其说明），调用方若把它放在自己的 try 里、在 finally
            //  里再 completeTracking，就会多完成一次：effectTrackDepth 变成负数、trackOpBit 错位，
            //  之后整个进程的依赖标记都是错的，而没有任何其他地方会报错。生产构建下代码被剔除。
            if (__DEV__) {
                assert(ReactiveEffect.activeScopes.lastIndexOf(this) !== -1, 'completeTracking without a matching prepareTracking on the scope stack (prepareTracking undoes itself when it throws: call it outside your try)')
            }
            if (this.useDepMarker && notifier.effectTrackDepth <= maxMarkerBits) {
                finalizeDepMarkers(this)
            }

            this.removeFromActiveScopes()
            Notifier.trackOpBit = 1 << --notifier.effectTrackDepth

        } else {
            if (isLast) {
                // CAUTION 活跃 effect 的 cleanup 不摘空 dep（见 cleanup：紧接着会 re-track 同一批 key）。
                //  async 收尾正是"先全退订、再重放本轮读到的"——上一轮读过、本轮不再读的 keyed dep
                //  若就此变空，没有人再回收它：按移动的 key 读（`map.get(key())`、`list.at(i())`）的
                //  长活 async computed 每换一个 key 就在长活 source 的 depsMap 里留下一个空 Dep
                //  （AGENTS「长活 source 上的订阅记账必须有界」）。重放之后把仍为空的摘掉；
                //  已销毁时 cleanup 自己已经摘过。只在 async 轮次收尾走一次，不在同步热路径上。
                const settled = this.active && this.deps.length ? this.deps.slice() : undefined
                this.cleanup()
                if (this._asyncTracks) {
                    // CAUTION destroy 后不得重放:挂起的 async/generator getter 完成时
                    //  才走到这里,若期间 effect 已销毁(cleanup 已在 destroy 中执行过),
                    //  重放会把已销毁 effect 重新订阅回各 dep——dep 对僵尸保持强引用
                    //  (真实泄漏),且每次源触发都白走一遍调度路径。与"destroy 取消
                    //  在途 async patch 的后续应用"(README §5)同一语义。
                    if (this.active) {
                        this._asyncTracks.forEach(track => track())
                    }
                    this._asyncTracks.length = 0
                }
                if (settled !== undefined) {
                    for (let i = 0; i < settled.length; i++) {
                        pruneEmptyDepFromHost(settled[i])
                    }
                }
            }

            this.removeFromActiveScopes()
        }

        // 栈内销毁的延迟执行点（见 static destroy 的 CAUTION）：此刻本段 run 的
        // marker/作用域栈已复原；仍在更外层栈上（嵌套/async 交错）时等下一个收尾。
        if (this._pendingDestroy !== 0 && !ReactiveEffect.activeScopes.includes(this)) {
            const flags = this._pendingDestroy
            this._pendingDestroy = 0
            ReactiveEffect.destroy(this, (flags & 2) !== 0, (flags & 4) !== 0)
        }
    }
    // 依赖触发时由 notifier 调用（非 session 路径）。基类 effect 不消费 TriggerInfo，
    // 直接重跑；Computed 覆写此方法，按 needsTriggerInfo 惰性组装 info。
    runFromTrigger(_source?: any, _type?: TriggerOpTypes, _inputInfo?: InputTriggerInfo) {
        this.run()
    }
    // primitive atom 写入的特化入口：newValue/oldValue 以标量传递，
    // 让最热的 atom 写路径完全不构造 info 对象。
    runFromAtomTrigger(_source?: any, _newValue?: unknown, _oldValue?: unknown) {
        this.run()
    }
    run(...args: any[]): any {
        // 一般用于调试
        if (!this.active) {
            return this.callGetter()
        }
        if (ReactiveEffect.activeScopes.includes(this)) {
            throw new Error('recursive effect call')
        }

        // FIXME 执行到一半的 generator 被再次 trigger 时：允许重入启动新一轮
        //  runGenerator（不在此处排队或取消）。Computed 靠 recomputeId 丢弃过期轮次的
        //  结果；基类 ReactiveEffect 的并发 generator 由调用方自行避免或容忍。
        //  （曾考虑排队/取消，与 Computed 的 force/recomputeId 模型冲突更大。）
        if (this.isRunningAsync) {
            // 有意空：不短路，落入下方重新启动 async 路径。
        }

        if(!this.isAsync) {
            // CAUTION dev 不变量：一次同步 run 结束后，全局作用域栈与追踪开关栈的
            //  深度必须复原。违约（漏 pop / 漏 reset）在这里当场炸，而不是在遥远的
            //  下游表现为静默的幽灵订阅或追踪失效。生产构建下代码被剔除。
            let scopesDepthBefore = 0
            let trackStackDepthBefore = 0
            if (__DEV__) {
                scopesDepthBefore = ReactiveEffect.activeScopes.length
                trackStackDepthBefore = notifier.trackStack.length
            }
            // prepareTracking 在 try 之外：失败时它自己撤销（见其说明），finally 若照常执行
            // 会多弹一层 trackStack。
            this.prepareTracking()
            notifier.enableTracking()
            try {
                return this.callGetter()
            } finally {
                notifier.resetTracking()
                this.completeTracking()
                if (__DEV__) {
                    assert(ReactiveEffect.activeScopes.length === scopesDepthBefore, 'activeScopes depth not restored after effect run (scope leak)')
                    assert(notifier.trackStack.length === trackStackDepthBefore, 'trackStack depth not restored after effect run (pause/reset imbalance)')
                }
            }
        } else {
            // async 执行中的时候产生了新的触发了重算怎么办？？？
            this.isRunningAsync = true
            const generator = this.callGetter() as Generator<any, string, boolean>
            const resultPromise = this.runGenerator(generator, (isFirst) => {
                this.prepareTracking(isFirst)
                notifier.enableTracking()
            }, (isLast) => {
                notifier.resetTracking()
                this.completeTracking()
            })

            resultPromise.then(() => {
                this.isRunningAsync = false
            }, () => {
                this.isRunningAsync = false
            })

            return resultPromise
        }
    }
    // notify recursive markDirty 时调用
    onDirty() {

    }
    // notify track 时调用
    onTrack(...args: any[]) {

    }
    onTrackDep(dep: any) {

    }

    cleanup() {
        const {deps} = this
        if (deps.length) {
            // CAUTION 只在销毁路径（active 已置 false）摘除空 dep：活跃 effect 的
            //  cleanup 是"重算前复位"（prepareTracking 的 full-cleanup、patch 的手动
            //  重注册），紧接着就会 re-track 同一批 key——此时摘除会把 dep 的复用
            //  变成每轮重算的 delete+重分配（findIndex patch 实测 -13% 吞吐）。
            //  无界增长的记账残留只来自"创建→销毁"的 churn（destroy 必经此处且
            //  active 为 false），在这里摘除即可封死该缺陷类。
            const prune = !this.active
            for (let i = 0; i < deps.length; i++) {
                const dep = deps[i]
                if (dep.delete(this)) {
                    trackRetainedDepEffectRemoved(dep)
                    if (prune) pruneEmptyDepFromHost(dep)
                }
            }
            deps.length = 0
        }
    }
    /**
     * 子类资源清理钩子：惰性 meta、lastCleanupFn、cleanPromise、行级 effect frame 等
     * 归子类所有的资源在这里释放。由静态 ReactiveEffect.destroy 核心在 active 置 false
     * 之后调用，所有销毁入口（实例 destroy()、destroyChildren、destroyComputed）都汇聚
     * 到该核心，钩子恰好执行一次。
     * CAUTION 子类清理逻辑必须放在本钩子（而不是 destroy() 覆写）里：destroy() 覆写
     *  只有直接调用实例方法时才执行，经 destroyChildren/destroyComputed 销毁时会被绕过。
     */
    destroyResources() {
    }
    /**
     * @internal
     * 把本 effect 在一次探测运行中捕获的 deps 与 children 原样转移给 target，
     * 自身清空。用于 RxList.map 的行级依赖探测：mapFn 只执行一次（在探测 effect 中），
     * 发现有依赖后把订阅关系转交给真正的行级 Computed，避免重跑 mapFn 的副作用。
     */
    transferCapturesTo(target: ReactiveEffect) {
        const deps = this.deps
        if (deps.length) {
            for (let i = 0; i < deps.length; i++) {
                const dep = deps[i]
                if (dep.delete(this)) trackRetainedDepEffectRemoved(dep)
                dep.add(target)
                trackRetainedDepEffectAdded(dep)
                target.addDep(dep)
            }
            deps.length = 0
        }
        const children = this._children
        if (children && children.length) {
            for (const child of children) {
                child.parent = target
            }
            if (target._children && target._children.length) {
                for (const child of children) {
                    child.index = target._children.length
                    target._children.push(child)
                }
            } else {
                target._children = children
                // index 保持原数组位置，无需修正
            }
            this._children = undefined
        }
    }
    destroy(ignoreChildren = false, fromParent = false) {
        ReactiveEffect.destroy(this, fromParent, ignoreChildren)
    }
    async runGenerator(generator: Generator<any, string, boolean>, beforeRun: (isFirst?:boolean) => any, afterRun: (isLast?:boolean) => any)   {
        // run generator，每次之前要调用 beforeRun，每次之后要调用 afterRun
        let isFirst = true
        let lastYieldValue: any = undefined
        while(true) {
            // CAUTION beforeRun 中如果要返回 false，一定要在所有操作之前，不然后面的 afterRun 执行不到，可能会导致一些内部状态无法重置。
            const implicitContinue = beforeRun(isFirst)
            isFirst = false
            if(implicitContinue === false) {
                closeAbandonedGenerator(generator)
                break
            }

            let value: any
            let done: boolean | undefined
            // CAUTION generator.next（用户代码）抛异常时也必须执行 afterRun 复位追踪状态
            try {
                ({value, done} = generator.next(lastYieldValue))
            } finally {
                afterRun(done)
            }
            lastYieldValue = value instanceof Promise ? await value : value
            if (done) break
        }
        return lastYieldValue
    }
}

// 恒定默认值放在原型上：实例只在真正改写时才产生自有属性（见类顶部 declare 说明）
// 基类的空钩子：销毁快路径用它判断「这个 effect 没有子类资源要释放」
const noDestroyResources = ReactiveEffect.prototype.destroyResources
// 销毁协议里其余可覆写的方法：被子类覆写时，快路径与 prepareTracking 的提前返回都不适用
const baseCleanup = ReactiveEffect.prototype.cleanup
const baseDestroyChildren = ReactiveEffect.prototype.destroyChildren
const baseDispatch = ReactiveEffect.prototype.dispatch
ReactiveEffect.prototype.isRunningAsync = false
ReactiveEffect.prototype._pendingDestroy = 0
ReactiveEffect.prototype.useDepMarker = true
ReactiveEffect.prototype.index = 0
ReactiveEffect.prototype.shouldCollectChild = true
ReactiveEffect.prototype.needsTriggerInfo = false
ReactiveEffect.prototype._inSession = false