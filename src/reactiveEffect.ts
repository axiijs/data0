import {Dep, finalizeDepMarkers, initDepMarkers} from "./dep.js";
import {maxMarkerBits, Notifier, notifier} from "./notify.js";
import type {InputTriggerInfo, TriggerInfo} from "./notify.js";
import type {TriggerOpTypes} from "./operations.js";
import {ManualCleanup} from "./manualCleanup.js";
import {isAsync, isGenerator} from "./util.js";
import {
    trackRetainedDepEffectAdded,
    trackRetainedDepEffectRemoved,
    trackRetainedReactiveEffectCreated,
    trackRetainedReactiveEffectDestroyed
} from "./retainedDiagnostics";


export class ReactiveEffect extends ManualCleanup {
    static activeScopes: ReactiveEffect[] = []
    public active: boolean
    // CAUTION isRunningAsync/useDepMarker/index/shouldCollectChild 的默认值放在原型上
    //  （见 class 定义后的赋值），实例只在真正改写时才产生自有属性。
    //  渲染框架里每个绑定都是一个 effect，这些"恒定默认值"的实例槽位在长列表下是可观的常驻内存。
    declare public isRunningAsync: boolean
    private _eventToCallbacks?: Map<string, Set<Function>>
    private _asyncTracks?: Array<() => void>
    private _children?: ReactiveEffect[]
    static destroy(effect: ReactiveEffect, fromParent = false, ignoreChildren = false) {
        if (!effect.active) return

        effect.cleanup()
        effect.active = false
        trackRetainedReactiveEffectDestroyed(effect)

        // 如果不是 fromParent，就要从父亲中移除。如果是，父亲会自己清空 children
        if (effect.parent && !fromParent) {
            // 要把自己从 parent.children 中移除掉。直接用 last 替换掉当前的要上出的，提升删除速度。
            const siblings = effect.parent._children
            if (siblings) {
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
            effect._children.length = 0
        }
        effect.dispatch('destroy')
    }

    deps: Dep[] = []
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
                ReactiveEffect.destroy(child, true)
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

    completeTracking(isLast = false, isAsync = this.isAsync) {
        if (!isAsync) {
            if (this.useDepMarker && notifier.effectTrackDepth <= maxMarkerBits) {
                finalizeDepMarkers(this)
            }

            ReactiveEffect.activeScopes.pop()
            Notifier.trackOpBit = 1 << --notifier.effectTrackDepth

        } else {
            if (isLast) {
                this.cleanup()
                if (this._asyncTracks) {
                    this._asyncTracks.forEach(track => track())
                    this._asyncTracks.length = 0
                }
            }

            ReactiveEffect.activeScopes.pop()
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

        // FIXME 执行到一般的 generator 如何处理？？应该形成队列还是直接取消？如果是 fullComputed，应该取消。
        //  如果是当成副作用，那么应该形成队列。
        if (this.isRunningAsync) {}

        if(!this.isAsync) {
            try {
                this.prepareTracking()
                notifier.enableTracking()
                return this.callGetter()
            } finally {
                notifier.resetTracking()
                this.completeTracking()
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
            for (let i = 0; i < deps.length; i++) {
                const dep = deps[i]
                if (dep.delete(this)) trackRetainedDepEffectRemoved(dep)
            }
            deps.length = 0
        }
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
                target.deps.push(dep)
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
    destroy(ignoreChildren = false) {
        ReactiveEffect.destroy(this, false, ignoreChildren)
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
ReactiveEffect.prototype.useDepMarker = true
ReactiveEffect.prototype.index = 0
ReactiveEffect.prototype.shouldCollectChild = true
ReactiveEffect.prototype.needsTriggerInfo = false
ReactiveEffect.prototype._inSession = false