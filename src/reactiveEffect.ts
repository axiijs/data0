import {Dep, finalizeDepMarkers, initDepMarkers} from "./dep.js";
import {maxMarkerBits, Notifier} from "./notify.js";
import {ManualCleanup} from "./manualCleanup.js";
import {isAsync, isGenerator} from "./util.js";
import {
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

        if (ReactiveEffect.activeScopes.length) {
            const parent = ReactiveEffect.activeScopes.at(-1)
            if (parent?.shouldCollectChild) {
                this.parent = parent
                this.index = parent.addChild(this)
            }
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
    dispatch(event: string, ...args: any[]) {
        const callbacks = this._eventToCallbacks?.get(event)
        if (callbacks) {
            callbacks.forEach(callback => callback.call(this, ...args))
        }
    }
    createGetterContext():any {
        return undefined
    }
    callGetter():any {

    }

    prepareTracking(isFirst = false, isAsync = this.isAsync) {
        if (!isAsync) {
            Notifier.trackOpBit = 1 << ++Notifier.instance.effectTrackDepth
            ReactiveEffect.activeScopes.push(this)

            if (this.useDepMarker && Notifier.instance.effectTrackDepth <= maxMarkerBits) {
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
            if (this.useDepMarker && Notifier.instance.effectTrackDepth <= maxMarkerBits) {
                finalizeDepMarkers(this)
            }

            ReactiveEffect.activeScopes.pop()
            Notifier.trackOpBit = 1 << --Notifier.instance.effectTrackDepth

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
                Notifier.instance.enableTracking()
                return this.callGetter()
            } finally {
                Notifier.instance.resetTracking()
                this.completeTracking()
            }
        } else {
            // async 执行中的时候产生了新的触发了重算怎么办？？？
            this.isRunningAsync = true
            const generator = this.callGetter() as Generator<any, string, boolean>
            const resultPromise = this.runGenerator(generator, (isFirst) => {
                this.prepareTracking(isFirst)
                Notifier.instance.enableTracking()
            }, (isLast) => {
                Notifier.instance.resetTracking()
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