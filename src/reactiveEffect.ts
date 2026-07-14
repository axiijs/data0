import {Dep, finalizeDepMarkers, initDepMarkers, pruneEmptyDepFromHost} from "./dep.js";
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
            effect._pendingDestroy = 1 | (fromParent ? 2 : 0) | (ignoreChildren ? 4 : 0)
            return
        }

        // CAUTION 先置 inactive 再执行清理：destroyResources 会运行用户 cleanup，
        //  其中的响应式写入若再触发本 effect（或重入 destroy），都以 active === false
        //  被安全拦截，不会出现"销毁中重算/重复销毁"。
        effect.active = false
        trackRetainedReactiveEffectDestroyed(effect)
        // CAUTION 子类资源清理钩子（惰性 meta、lastCleanupFn、cleanPromise、行级
        //  effect frame 等）。所有销毁入口——实例 destroy()、destroyChildren、
        //  destroyComputed——都汇聚到本静态核心，钩子恰好执行一次。旧实现把这些
        //  清理放在各子类的 destroy() 覆写里，destroyChildren/destroyComputed 走
        //  静态函数时全部被绕过（子 computed 的 onCleanup 从不执行、惰性 meta 泄漏）。
        effect.destroyResources()
        effect.cleanup()

        // 如果不是 fromParent，就要从父亲中移除。如果是，父亲会自己清空 children
        if (effect.parent && !fromParent) {
            // 要把自己从 parent.children 中移除掉。直接用 last 替换掉当前的要上出的，提升删除速度。
            const siblings = effect.parent._children
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
        if (effect.parent !== undefined) effect.parent = undefined
        if (!ignoreChildren) {
            effect.destroyChildren()
        } else if (effect._children) {
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
        effect.dispatch('destroy')
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
            if (parent.shouldCollectChild) {
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
        if (!children) return
        if (children.length) {
            children.forEach(child => {
                // CAUTION 走实例方法（fromParent=true）：destroy 可能被子类覆写，
                //  静态调用会绕过覆写与 destroyResources 钩子链。
                child.destroy(false, true)
            })
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

    prepareTracking(isFirst = false, isAsync = this.isAsync) {
        if (!isAsync) {
            Notifier.trackOpBit = 1 << ++notifier.effectTrackDepth
            ReactiveEffect.activeScopes.push(this)

            if (this.useDepMarker && notifier.effectTrackDepth <= maxMarkerBits) {
                initDepMarkers(this)
            } else {
                this.cleanup()
            }

            this.destroyChildren()

        } else {
            // async 模式下是通过暂存一个 track 函数到 asyncTracks 中，然后在 completeTracking 时执行。
            // 所以这里只需要 push scope 就行了。
            ReactiveEffect.activeScopes.push(this)
            if (isFirst) {
                if (this._asyncTracks) this._asyncTracks.length = 0
                this.destroyChildren()
            }
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
            if (this.useDepMarker && notifier.effectTrackDepth <= maxMarkerBits) {
                finalizeDepMarkers(this)
            }

            this.removeFromActiveScopes()
            Notifier.trackOpBit = 1 << --notifier.effectTrackDepth

        } else {
            if (isLast) {
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
            try {
                this.prepareTracking()
                notifier.enableTracking()
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
            if(implicitContinue === false) break

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
ReactiveEffect.prototype.isRunningAsync = false
ReactiveEffect.prototype._pendingDestroy = 0
ReactiveEffect.prototype.useDepMarker = true
ReactiveEffect.prototype.index = 0
ReactiveEffect.prototype.shouldCollectChild = true
ReactiveEffect.prototype.needsTriggerInfo = false
ReactiveEffect.prototype._inSession = false