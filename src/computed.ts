import {getDebugName,} from "./debug";
import {InputTriggerInfo, Notifier, notifier, TriggerInfo} from './notify'
import {assert, isAsync, isGenerator, nextTick, warn} from "./util";
import {Atom, atom, isAtom, isPrimitiveAtom} from "./atom";
import {ReactiveEffect} from "./reactiveEffect.js";
import {TrackOpTypes, TriggerOpTypes} from "./operations.js";
import {CleanupFrame} from "./manualCleanup";
import {
    markRetainedReactiveEffectKind,
    setRetainedReactiveEffectSource,
    trackRetainedReactiveEffectCreated
} from "./retainedDiagnostics";
import {restoreEffectDeps} from "./dep.js";

export const computedToInternal = new WeakMap<any, Computed>()

export type CallbacksType = {
    onRecompute?: (data: any) => void,
    onCleanup?: (data: any) => void,
    onPatch?: (t: Computed) => void,
    onDestroy?: (t: ReactiveEffect) => void,
    onTrack?: Parameters<ReactiveEffect["on"]>[1],
}


export type ComputedData<T = any> = Atom<T> | T
export type SimpleApplyPatchType<T> = (computedData: ComputedData, info: TriggerInfo[]) => any
export type AsyncApplyPatchType<T> = (computedData: ComputedData, info: TriggerInfo[]) => Promise<any>
export type GeneratorApplyPatchType<T> = (computedData: ComputedData, info: TriggerInfo[]) => Generator<any, string, boolean>
export type ApplyPatchType<T = any> = SimpleApplyPatchType<T> | AsyncApplyPatchType<T> | GeneratorApplyPatchType<T>



export type GetterContext<T = any> = {
    lastValue: ComputedData<T>,
    onCleanup: (fn: () => any) => void,
    pauseCollectChild: () => void,
    resumeCollectChild: () => void,
    asyncStatus: Atom<null | boolean | string>,
}

export type GetterType<T = any> = (context: GetterContext) => T | Generator<T | Promise<T>, T | Promise<T>, boolean>
export type DirtyCallback = (recompute: (force?: boolean) => void, markDirty: () => any, infos?: any[]) => void
export type SkipIndicator = { skip: boolean }


export function destroyComputed(computedItem: ComputedData) {
    const internal = computedToInternal.get(computedItem)!
    // CAUTION 走实例方法而不是静态 ReactiveEffect.destroy：子类（RxList/RxMap/RxSet）
    //  可能覆写 destroy 附加行为，绕过实例方法会跳过它们。
    internal.destroy()
}

export function getComputedInternal(computedItem: ComputedData) {
    return computedToInternal.get(computedItem)
}

export function setComputedRetainedDiagnosticSource(computedItem: ComputedData, source: string) {
    const internal = computedToInternal.get(computedItem)
    if (internal) setRetainedReactiveEffectSource(internal, source)
}

const queuedRecomputes = new WeakSet<Computed>()

// 调度上下文（microtask/nextTick）没有可以同步传播异常的调用方：
// 1. 一定要先出队再执行，否则 recompute 抛错（同步 getter 的异常会重抛）会把
//    computed 永久卡在 queuedRecomputes 里，之后永远无法再被调度；
//    先出队还保证执行期间的新触发能重新入队，而不是被去重静默吞掉。
// 2. 异常必须捕获并上报（而不是变成 microtask 的 uncaught exception 崩掉进程）。
//    computed 已经由 handleRecomputeError 复位为 DIRTY，下次触发可以重试。
function runScheduledRecompute(this: Computed, recompute: (force?: boolean) => void) {
    queuedRecomputes.delete(this)
    try {
        recompute()
    } catch (err) {
        console.error('[data0] uncaught error in scheduled recompute:', err)
    }
}

// 如果是 async 的，用 queueMicrotask 来调度。
// 如果不是 async 的，用 markDirty 而不是直接 recompute
export function scheduleNextMicroTask(this: Computed, recompute: (force?: boolean) => void, markDirty: () => any) {
    if (queuedRecomputes.has(this)) return
    queuedRecomputes.add(this)
    queueMicrotask(() => runScheduledRecompute.call(this, recompute))
}


export function scheduleNextTick(this: Computed, recompute: (force?: boolean) => void, markDirty: () => any) {
    if (queuedRecomputes.has(this)) return
    queuedRecomputes.add(this)
    nextTick(() => runScheduledRecompute.call(this, recompute))
}

export const STATUS_DIRTY = -1
export const STATUS_RECOMPUTING_DEPS = 1
export const STATUS_RECOMPUTING = 2
export const STATUS_CLEAN = 3

export type StatusType = typeof STATUS_CLEAN | typeof STATUS_DIRTY | typeof STATUS_RECOMPUTING_DEPS | typeof STATUS_RECOMPUTING

export const  FULL_RECOMPUTE_PHASE = 1
export const  PATCH_PHASE = 2
type Phase = typeof FULL_RECOMPUTE_PHASE | typeof PATCH_PHASE

// async 重算的代次 id：只用于新旧比较，自增整数即可（旧实现用随机字符串 uuid，
// 每次 async 重算分配一个字符串）
let nextRunEffectId = 1

/**
 * 计算和建立依赖过程。这里因为要支持 async / patch 模式，所以完全覆盖了 ReactiveEffect 的行为。
 * 1. 无 patch 模式，全量计算，每次都会重新收集依赖。
 *   1.1 第一次 callAutoTrackGetter
 *   1.2 重算 recompute -> callAutoTrackGetter
 * 2. patch 模式，增量计算
 *   2.1 第一次 callManualTrackGetter
 *   2.2 重算 recompute -> applyPatch
 *   2.3 强制重算 recompute(true) -> callManualTrackGetter
 *
 * @category Basic
 */
export class Computed extends ReactiveEffect {
    data: ComputedData
    trackClassInstance = false
    immediate = false
    // recomputing = false
    isAsync = false
    isPatchAsync? = false
    inPatch = false
    phase: Phase  = FULL_RECOMPUTE_PHASE
    runtEffectId?: number
    asyncStatus?: Atom<null | boolean | string>
    // CAUTION status 惰性 atom 化：内部状态机只读写 _status 数字字段。
    //  一次重算要写 status 三次（DIRTY→RECOMPUTING→CLEAN），每次 atom 写都有
    //  Object.is + trigger 调用 + info 对象分配；而绝大多数 Computed 从来没有人
    //  响应式地读 status。只有外部真正访问 .status 时才创建 atom 并保持同步
    //  （与 updatedAt 的惰性化同款手法）。
    _status!: StatusType
    declare _statusAtom?: Atom<StatusType>
    get status(): Atom<StatusType> {
        return this._statusAtom ?? (this._statusAtom = atom(this._status))
    }
    setStatus(next: StatusType) {
        if (this._status === next) return
        this._status = next
        this._statusAtom?.(next)
    }

    // CAUTION 下面的集合/atom 字段全部惰性分配：一个 Computed（以及继承它的 RxList/RxMap/RxSet）
    //  原来无条件预分配 3 个数组、2 个 Set、1 个 WeakMap、1 个 Map、1 个 updatedAt atom，
    //  每实例 ~1KB 的常驻内存，多数场景根本用不到。
    _triggerInfos?: TriggerInfo[]
    get triggerInfos(): TriggerInfo[] {
        return this._triggerInfos ?? (this._triggerInfos = [])
    }
    scheduleRecompute?: DirtyCallback
    // 自定义调度器是否声明了第三个参数（triggerInfos）。内置调度器不消费，
    // 不声明就不做数组拷贝。默认值在原型上。
    declare scheduleNeedsInfos: boolean
    // 用来 patch 模式下，收集新增和删除是产生的 effectFrames
    _effectFramesArray?: ReactiveEffect[][]
    get effectFramesArray(): ReactiveEffect[][] {
        return this._effectFramesArray ?? (this._effectFramesArray = [])
    }
    _keyToEffectFrames?: WeakMap<any, ReactiveEffect[]>
    get keyToEffectFrames(): WeakMap<any, ReactiveEffect[]> {
        return this._keyToEffectFrames ?? (this._keyToEffectFrames = new WeakMap())
    }
    manualTracking = false
    // TODO 需要一个更好的约定
    public get debugName() {
        return getDebugName(this.data)
    }
    public static id = 0
    public id: number = Computed.id++
    public isAsyncGetter: boolean = false
    public isGeneratorGetter: boolean = false
    public isAsyncPatch: boolean = false
    public isGeneratorPatch: boolean = false
    // updatedAt 惰性化：时间戳先记在普通字段上，只有真正被读（想要响应式订阅）时才创建 atom
    _updatedAtTime?: number
    _updatedAt?: Atom<number|undefined>
    public get updatedAt(): Atom<number|undefined> {
        return this._updatedAt ?? (this._updatedAt = atom(this._updatedAtTime))
    }
    setUpdatedAt(time: number) {
        this._updatedAtTime = time
        if (this._updatedAt) this._updatedAt(time)
    }
    // CAUTION 可选构造参数不再用参数属性（public xxx）声明：参数属性会无条件产生
    //  own property 赋值，即使值是 undefined 也占一个实例槽位。这里全部改成
    //  "有值才赋"，默认值放在原型上（见 class 定义后的赋值）。
    //  getter 复用 ReactiveEffect 里同样的条件赋值，不再重复声明。
    declare getter?: GetterType
    declare applyPatch?: ApplyPatchType
    declare callbacks?: CallbacksType
    declare skipIndicator?: SkipIndicator
    declare preventEffectSession: boolean
    constructor(
        getter?: GetterType,
        applyPatch?: ApplyPatchType,
        scheduleRecompute?: DirtyCallback|true,
        callbacks?: CallbacksType,
        skipIndicator?: SkipIndicator,
        preventEffectSession?: boolean
    ) {
        super(getter)
        if (applyPatch !== undefined) this.applyPatch = applyPatch
        if (callbacks !== undefined) this.callbacks = callbacks
        if (skipIndicator !== undefined) this.skipIndicator = skipIndicator
        if (preventEffectSession) this.preventEffectSession = true
        this._status = typeof getter === 'function' ? STATUS_DIRTY : STATUS_CLEAN
        // CAUTION 回调注册对源模式（无 getter）与计算模式一视同仁：
        //  源模式结构的 onDestroy/onTrack 同样是有效契约。
        if (callbacks?.onDestroy) this.on('destroy', callbacks.onDestroy)
        if (callbacks?.onTrack) this.on('track', callbacks.onTrack)
        if (callbacks?.onRecompute) this.on('recompute', callbacks.onRecompute)
        if (callbacks?.onCleanup) this.on('cleanup', callbacks.onCleanup)

        if (!getter) {
            // CAUTION 源模式（无 getter 的 RxList/RxMap/RxSet 以及占位 Computed）也是
            //  完整的生命周期对象：active 必须为 true，否则静态 ReactiveEffect.destroy
            //  对 inactive 直接 return——destroy 事件从不派发、children/资源从不清理。
            //  历史缺陷：filter() 把内部 mapList 的销毁挂在 filtered.on('destroy') 上，
            //  filtered 是源模式列表，destroy() 完全无效（僵尸更新 + 订阅泄漏）。
            //  这里补上 base 构造器里因无 getter 而跳过的创建登记，销毁登记对称。
            this.active = true
            trackRetainedReactiveEffectCreated(this)
            markRetainedReactiveEffectKind(this, 'Computed', this.getRetainedDiagnosticSource())
            return
        }
        markRetainedReactiveEffectKind(this, 'Computed', this.getRetainedDiagnosticSource())

        this.isAsyncGetter = isAsync(getter)
        this.isGeneratorGetter = isGenerator(getter)

        if (this.isAsyncGetter || this.isGeneratorGetter) {
            this.isAsync = true
            this.asyncStatus = atom(null)
        }

        this.manualTracking = !!applyPatch
        if (this.applyPatch) {
            // 有 patch 时，dep 的增量 track 也是自己完成的。
            this.useDepMarker = false
            this.isAsyncPatch = isAsync(this.applyPatch)
            this.isGeneratorPatch = isGenerator(this.applyPatch)
            this.isPatchAsync = this.isAsyncPatch|| this.isGeneratorPatch
            // CAUTION generator patch 会读写 asyncStatus，但 asyncStatus 原来只在
            //  getter 为 async/generator 时创建：同步 getter + generator patch 的组合
            //  第一次 patch 就会 TypeError，且状态回 DIRTY 后每次重试都再次崩溃，
            //  派生数据从此永久陈旧。
            if (this.isPatchAsync && !this.asyncStatus) {
                this.asyncStatus = atom(null)
            }
        }

        if (typeof scheduleRecompute === 'function') {
            this.scheduleRecompute = scheduleRecompute
            // 内置调度器（scheduleNextMicroTask/scheduleNextTick）只声明两个参数，
            // 只有声明了第三个参数的自定义调度器才需要 triggerInfos 拷贝
            if (scheduleRecompute.length > 2) this.scheduleNeedsInfos = true
        } else if(this.isAsync && scheduleRecompute !== true) {
            // async 默认用 nextTick 来调度，但是可以通过传递 true 来强制立即执行。
            this.scheduleRecompute = scheduleNextMicroTask
        } else {
            this.immediate = true
        }

        // 只有 patch 型 computed（applyPatch 消费 triggerInfos）和要求 infos 的自定义
        // 调度器需要 trigger 路径构造 info 对象；其余情况 notifier 走零分配路径。
        if (this.applyPatch || this.scheduleNeedsInfos) this.needsTriggerInfo = true
    }
    getRetainedDiagnosticSource() {
        const constructorName = this.constructor?.name || 'Computed'
        const getterName = this.getter?.name
        return getterName ? `${constructorName}.${getterName}` : constructorName
    }
    runEffect() {
        let getterResult

        if (this.isAsync) {
            const runEffectId = nextRunEffectId++
            this.runtEffectId = runEffectId

            // 说明上一次的还在执行中！，立即设为 false，再重新开始
            if(this.asyncStatus!.raw) {
                this.asyncStatus!(false)
            }
            this.asyncStatus!(true)
            getterResult = this.isGeneratorGetter ? this.callGeneratorGetter(runEffectId) : this.callAsyncGetter()
            getterResult.then((data:any) => {
                if (this.runtEffectId !== runEffectId) return false

                // this.replaceData(data)
                this.asyncStatus!(false)
                return data
            }, (err: any) => {
                // 出错时也要复位 asyncStatus；错误本身由 fullRecompute 的 rejection 分支处理。
                if (this.runtEffectId !== runEffectId) return
                this.asyncStatus!(false)
            })
        } else {
            getterResult = this.callSimpleGetter()
            // this.replaceData(getterResult)
        }

        return getterResult
    }
    callAsyncGetter(): any {
        const getterContext = this.createGetterContext()
        warn('async getter can only track reactive data before first await. If you want to track more data, please use generator getter.')
        this.prepareTracking(true)
        this.manualTracking ? notifier.pauseTracking() : notifier.enableTracking()
        try {
            return this.getter!.call(this, getterContext!)
        } finally {
            notifier.resetTracking()
            this.completeTracking(true)
        }
    }
    callGeneratorGetter(id:number) {
        const runEffectId = id
        const getterContext = this.createGetterContext()
        return this.runGenerator(
            this.getter!.call(this, getterContext!),
            (isFirst) => {
                if (runEffectId !== this.runtEffectId) return false

                this.prepareTracking(isFirst)
                this.manualTracking ? notifier.pauseTracking() : notifier.enableTracking()
            },
            (isLast) => {
                notifier.resetTracking()
                this.completeTracking(isLast)
            }
        )
    }
    callSimpleGetter() {
        const getterContext = this.createGetterContext()
        this.prepareTracking()
        this.manualTracking ? notifier.pauseTracking() : notifier.enableTracking()
        // CAUTION 一定要 try/finally：用户 getter 抛异常时必须复位全局追踪状态
        //  （activeScopes/effectTrackDepth/trackOpBit），否则一次异常会永久污染整个系统。
        try {
            return this.getter!.call(this, getterContext!)
        } finally {
            notifier.resetTracking()
            this.completeTracking()
        }
    }

    // CAUTION context 对象整体复用：带 context 参数的 getter（如 filter 的每行 computed）
    //  每次重算都要走这里，原实现每次分配 1 个对象 + 1 个闭包 + 2 个 bind。
    //  context 的契约是"仅在 getter 执行期间有效"，跨次复用安全；lastValue 每次刷新。
    declare _getterContext?: GetterContext
    createGetterContext(): GetterContext | undefined {
        if (!this.getter || this.getter.length === 0) return undefined
        const cached = this._getterContext
        if (cached) {
            cached.lastValue = this.data
            return cached
        }
        return this._getterContext = {
            lastValue: this.data,
            onCleanup: (fn: () => any) => this.lastCleanupFn = fn,
            asyncStatus: this.asyncStatus!,
            pauseCollectChild: () => this.pauseCollectChild(),
            resumeCollectChild: () => this.resumeCollectChild(),
        }
    }

    callGetter() {
        const getterContext = this.createGetterContext()
        return this.getter!.call(this, getterContext!)
    }


    // 这是传递给外部 scheduleRecompute 的，用来代理 notify 上的 recursiveMarkDirty
    _boundRecursiveMarkDirty?: () => void
    get boundRecursiveMarkDirty() {
        return this._boundRecursiveMarkDirty ?? (this._boundRecursiveMarkDirty = () => this.recursiveMarkDirty())
    }
    recursiveMarkDirty() {
        // CAUTION notifier.getDepEffects 给的是去重的 Effect, 不然这里会触发多次无意义的 run
        // 旧实现还会把双向引用记进 dirtyFromDeps/markedDirtyEffects 两个 Set：
        // 它们从无任何读取方，却对已销毁的 effect 保持强引用（纯泄漏），已移除。
        const depEffects = notifier.getDepEffects(this.trackClassInstance ? this: this.data)
        if (!depEffects) return

        for(const effect of depEffects) {
            effect.run()
        }
    }
    // CAUTION cleanPromise 惰性化：每轮 dirty 都无条件创建 Promise + 闭包，
    //  而绝大多数 computed 从来没有人 await。改为：进入"未完成的计算周期"时只置
    //  一个布尔（_cleanExpected），真正的 Promise 在第一次读取 .cleanPromise 时才创建。
    //  周期结束（resolve/reject）语义与旧实现一致。顺带修复旧实现的一个隐患：
    //  无人持有 cleanPromise 时 async 出错会产生 unhandled rejection。
    declare _cleanExpected: boolean
    declare _cleanPromise?: Promise<any>
    resolveCleanPromise?: (value?: any) => any
    rejectCleanPromise?: (value?: any) => any
    get cleanPromise(): Promise<any> | undefined {
        if (this._cleanPromise) return this._cleanPromise
        if (!this._cleanExpected) return undefined
        this.createCleanPromise()
        return this._cleanPromise
    }
    // 进入一个"将来会 settle"的计算周期。真正创建 Promise 推迟到读取时。
    expectCleanPromise() {
        this._cleanExpected = true
    }
    settleCleanPromise() {
        this._cleanExpected = false
        this.resolveCleanPromise?.()
    }
    settleCleanPromiseWithError(err: any) {
        this._cleanExpected = false
        this.rejectCleanPromise?.(err)
    }
    createCleanPromise() {
        this._cleanExpected = true
        const cleanAll = () => {
            delete this._cleanPromise
            delete this.resolveCleanPromise
            delete this.rejectCleanPromise
        }
        this._cleanPromise = new Promise((res, rej) => {
            this.resolveCleanPromise = (value:any) => {
                res(value)
                cleanAll()
            }
            this.rejectCleanPromise = (value:any) => {
                rej(value)
                cleanAll()
            }
        })
        // CAUTION 预挂一个 no-op 的 rejection handler：cleanPromise 被创建后调用方可能并不
        //  await（例如调用 recompute() 后丢弃返回值），async 出错时不能变成 unhandled
        //  rejection（Node >= 15 默认直接崩溃进程）。真正 await 的调用方挂在同一个
        //  promise 上，依然会正常收到 rejection。
        this._cleanPromise.catch(() => {})
    }
    // dep trigger/recursiveMarkDirty/onTrack 时调用。
    // 1. 没有 infos 和 immediate 说明是 markDirty，是否启动由自己决定
    // 2. 有 infos 说明是 dep trigger，是否启动由自己决定
    // 3. 没有 infos 但有 immediate 是 onTrack 的强制启动，可能是初始化时。
    run(infos?: TriggerInfo[], immediate = false) {
        if (this.skipIndicator?.skip) return
        if (infos && infos.length) {
            // CAUTION 不能 push(...infos)：batch 中积累的 infos 可能超过引擎实参上限
            //  （约 65k，超过直接 RangeError），必须逐个 push（同 spliceMany 的动机）。
            const triggerInfos = this.triggerInfos
            for (let i = 0; i < infos.length; i++) {
                triggerInfos.push(infos[i])
            }
        }
        this.handleTriggered(immediate)
    }
    // trigger 热路径入口（见 ReactiveEffect.runFromTrigger 的说明）：
    // info 只在本 computed 真正消费时才构造。
    runFromTrigger(source: any, type: TriggerOpTypes, inputInfo?: InputTriggerInfo) {
        if (this.skipIndicator?.skip) return
        if (this.needsTriggerInfo) {
            this.triggerInfos.push((inputInfo ? {...inputInfo, source, type} : {source, type}) as TriggerInfo)
        }
        this.handleTriggered(false)
    }
    runFromAtomTrigger(source: any, newValue?: unknown, oldValue?: unknown) {
        if (this.skipIndicator?.skip) return
        if (this.needsTriggerInfo) {
            this.triggerInfos.push({source, type: TriggerOpTypes.ATOM, key: 'value', newValue, oldValue} as TriggerInfo)
        }
        this.handleTriggered(false)
    }
    private handleTriggered(immediate: boolean) {
        // markDirty, initial 状态不需要 mark dirty
        if (this._status === STATUS_CLEAN) {
            this.dispatch('dirty')
            this.setStatus(STATUS_DIRTY)
        }


        // 循环检测，是 sync computed 已经在重算中了又立刻重算，说明重算中又有触发依赖变更的代码。
        // 要么把依赖变更代码移出去，要么把 computed 用 schedule 延迟一下。
        // CAUTION async patch（isPatchAsync && inPatch）挂起期间收到新触发是合法的：
        //  info 会累积到 triggerInfos，由 runAsyncPatch/runGeneratorPatch 的 while 循环
        //  或 finishPatchRecompute 的续跑消化，不属于同步重算环。
        assert(
            !((immediate || this.immediate) && this._status > STATUS_DIRTY && !this.isAsync && !(this.isPatchAsync && this.inPatch)),
            'detect recompute triggerred in sync recompute, move trigger code to next tick or it may lead to infinite loop'
        )

        // 哪些情况可能出现 recomputing 过程中又触发了 run :
        // 1. 在 lazy recompute 模式下，可能出现依赖是一个 atomComputed，
        //  触发它的重算时会使得 atom trigger 重新触发 run，这个时候我们已经在 recomputing 了，
        //  只需要获取 info 就行了，不需要再次触发 recompute/schedule 了。
        // 2. 在 async 模式下，任何依赖都可以再触发 recompute。
        // (强制)立刻执行 或者 已经在 recompute 中的 async, 会立刻重算。
        if (immediate || this.immediate || (this._status > STATUS_DIRTY && this.isAsync)) {
            this.recomputeInternal()
        } else {
            // CAUTION 如果是在 sync 的 recompute 阶段触发的。
            //  例如在 autorun/once 里面可能会既依赖 computed, 产生了 computed 变化，用户自己系统通过这种方式达到一个平衡状态。
            //   例如 不断将一个 pending list 中的数据取出来变成 processing。
            //   这时候的第一次 run 会变成 clean，所以 schedule 的 recompute 一定要是 forceRecompute 才能继续执行。
            const recompute = (this._status > STATUS_DIRTY && !this.isAsync) ? () => this.recomputeInternal(true) : this.boundRecompute
            if (this.scheduleNeedsInfos) {
                this.scheduleRecompute!(recompute, this.boundRecursiveMarkDirty, [...(this._triggerInfos ?? [])])
            } else {
                this.scheduleRecompute!(recompute, this.boundRecursiveMarkDirty)
            }
        }

        // 如果不是已经开始重算或者立刻开始计算，那么从标记为脏也要标记 cleanPromise 周期开始
        // 如果在 scheduleRecompute 或者 recompute 已经开始，那么由里面判断是否要建立 cleanPromise
        if (this._status === STATUS_DIRTY) {
            this.expectCleanPromise()
        }
    }

    prepareRecompute() {
        this.setStatus(STATUS_RECOMPUTING)
        // 可以用于清理一些用户自己的副作用。
        // 这里用了两个名字，onCleanup 是为了和 rxList 中的 api 一致。
        // onRecompute 可以用作 log 等其他副作用
        this.dispatch('recompute', this.data)
        this.dispatch('cleanup', this.data)
        // 使用 context 注册的 cleanup
        // CAUTION 必须先复位再调用：cleanup 只该执行一次。若本轮 getter 不再注册新的
        //  onCleanup（条件注册的场景），复位失败会导致同一个 cleanup 在后续每轮重算
        //  被重复调用（对连接关闭/引用计数类资源是 double-free）。
        if (this.lastCleanupFn) {
            const cleanup = this.lastCleanupFn
            this.lastCleanupFn = undefined
            cleanup()
        }
    }

    public recomputeId: number = 0
    // 重算过程中抛异常时统一处理：回到 DIRTY（后续 trigger 还能重试），
    // reject cleanPromise 让 await 方拿到错误，并派发 error 事件。
    // willPropagate 为 true 表示调用方随后会同步 rethrow（错误必然可观测）；
    // 否则（async 路径）在既无 cleanPromise 等待方也无 error 监听者时，
    // 用 console.error 兜底上报，避免错误被完全静默吞掉。
    handleRecomputeError(err: any, willPropagate = false) {
        this.inPatch = false
        // CAUTION patch 轮次抛错后增量状态不可信：该轮 triggerInfos 已被消费
        //  （runSimplePatch 的 finally / runAsyncPatch 的轮首快照），且 applyPatch
        //  可能已部分应用。若 phase 停留在 PATCH_PHASE，下次触发只会增量重放
        //  新 info——抛错那轮的变更在派生数据里**永久缺失**（静默分叉，违反
        //  "派生 ≡ 全量重算"不变量）。这里统一回退到 FULL_RECOMPUTE_PHASE 并清空
        //  残留 info：下次触发走全量重算，错误恢复后结果必然与终态 source 一致。
        //  （fullRecompute 错误路径本来就处于 FULL 阶段，置位幂等无害。）
        if (this._triggerInfos) this._triggerInfos.length = 0
        this.phase = FULL_RECOMPUTE_PHASE
        this.setStatus(STATUS_DIRTY)
        const observable = this._cleanPromise !== undefined || this.hasListener('error')
        this.settleCleanPromiseWithError(err)
        this.dispatch('error', err)
        if (!willPropagate && !observable) {
            console.error('[data0] uncaught error in async computed recompute:', err)
        }
    }
    finishFullRecompute(result: any) {
        if (!this.preventEffectSession) {
            notifier.createEffectSession()
        }

        this.replaceData(result)
        this.setStatus(STATUS_CLEAN)
        this.setUpdatedAt(Date.now())

        if (this.applyPatch) {
            this.phase = PATCH_PHASE
        }
        if (!this.preventEffectSession) {
            notifier.digestEffectSession()
        }
        this.settleCleanPromise()
        this.dispatch('clean')
    }
    // CAUTION 同步 computed 走完全同步的路径：这样用户 getter 的异常能同步抛到
    //  触发变更的调用点（而不是变成 unhandled rejection），语义上和"非 async 就应该同步计算"一致。
    fullRecompute(): any {
        const recomputeId = ++this.recomputeId
        // 失败重算必须恢复“上一次成功”的完整依赖集合。prepareTracking 可能在
        // getter 第一次读取前就清掉旧 deps，或在抛错前只收集了一部分新 deps。
        const previousDeps = this.deps.length ? this.deps.slice() : undefined
        this.inPatch = false
        // 每次 full recompute 清空所有的 triggerInfos，这样才能使 patchable recompute 不错乱。
        if (this._triggerInfos) this._triggerInfos.length = 0

        this.prepareRecompute()
        // 默认行为，重算并且重新收集依赖
        // CAUTION 用户一定要自己保证在第一次 await 之前读取了所有依赖。
        if (this.isAsync) {
            this.expectCleanPromise()

            return Promise.resolve(this.runEffect()).then((result: any) => {
                // 在 async fullRecompute 时，是有可能因为 dep 变化触发新的 trigger 的和新的 fullRecompute 的。
                // 这时就会 recomputeId 不一致，老的 recompute 就不用管了。
                if (this.recomputeId !== recomputeId) return
                this.finishFullRecompute(result)
            }, (err: any) => {
                if (this.recomputeId !== recomputeId) return
                restoreEffectDeps(this, previousDeps)
                this.handleRecomputeError(err)
            })
        }

        let result: any
        try {
            result = this.runEffect()
        } catch (err) {
            // CAUTION recomputeId 守卫：getter 内同步重入 destroy（延迟销毁在
            //  completeTracking 已 flush）后 recomputeId 已推进——此时绝不能
            //  restoreEffectDeps（会把已销毁 effect 重新订阅成僵尸），也不该再写状态。
            if (this.recomputeId === recomputeId) {
                restoreEffectDeps(this, previousDeps)
                this.handleRecomputeError(err, true)
            }
            throw err
        }
        if (this.recomputeId !== recomputeId) return
        this.finishFullRecompute(result)
    }
    finishPatchRecompute(patchResult: any): any {
        // explicit return false 说明出现了无法 patch 的情况，表示一定要重算
        if (patchResult === false) {
            if (this._triggerInfos) this._triggerInfos.length = 0
            this.inPatch = false
            // fullRecompute 会推进 recomputeId 并负责自己的收尾（status/cleanPromise 等）。
            return this.fullRecompute()
        }

        this.inPatch = false
        // CAUTION async patch 收尾竞态：runAsyncPatch 的 while 检查到 triggerInfos 为空
        //  之后、finish 之前（微任务间隙）仍可能到达新触发——handleTriggered 看到 inPatch
        //  为 true 只会排队 info。这里必须续跑一轮 patch，否则该 info 要等下一次无关
        //  触发才被消化（静默陈旧窗口）。
        if (this.isPatchAsync && this._triggerInfos && this._triggerInfos.length) {
            return this.patchRecompute()
        }
        this.setStatus(STATUS_CLEAN)
        this.setUpdatedAt(Date.now())
        this.sendTriggerInfos()
        this.dispatch('clean')
        this.settleCleanPromise()
    }
    patchRecompute(): any {
        this.inPatch = true
        // patch 也使用 recomputeId 是因为要判断是否被强制 fullRecompute 打断
        const recomputeId = ++this.recomputeId

        this.dispatch('recomputeDeps')

        this.prepareRecompute()

        if (this.isPatchAsync) {
            this.expectCleanPromise()

            return Promise.resolve(this.runPatch()).then((patchResult: any) => {
                // 虽然 patch 的 recompute 是串行的，但是有可能被用户强制的 fullRecompute 打断。
                // 这个时候就不用管了。destroy 会推进 recomputeId（destroyResources），
                // 因此已销毁实例的在途收尾也在这里被丢弃。
                if (recomputeId !== this.recomputeId) return
                return this.finishPatchRecompute(patchResult)
            }, (err: any) => {
                if (recomputeId !== this.recomputeId) return
                this.handleRecomputeError(err)
            })
        }

        let patchResult: any
        try {
            patchResult = this.runPatch()
        } catch (err) {
            // recomputeId 守卫：patch 内同步重入 destroy（延迟销毁已 flush）后
            //  不再回写状态/cleanPromise（destroy 已 settle）。
            if (recomputeId === this.recomputeId) this.handleRecomputeError(err, true)
            throw err
        }
        if (recomputeId !== this.recomputeId) return
        return this.finishPatchRecompute(patchResult)
    }
    _savedTriggerInfos?: Parameters<Notifier["trigger"]>[]
    get savedTriggerInfos(): Parameters<Notifier["trigger"]>[] {
        return this._savedTriggerInfos ?? (this._savedTriggerInfos = [])
    }
    trigger(...args: Parameters<Notifier["trigger"]>) {
        this.savedTriggerInfos.push(args)
    }
    sendTriggerInfos() {
        if (!this._savedTriggerInfos?.length) return
        const infos = [...this._savedTriggerInfos]
        this._savedTriggerInfos.length = 0
        notifier.createEffectSession()
        try {
            for(const info of infos) {
                notifier.trigger(...info)
            }
        } finally {
            notifier.digestEffectSession()
        }
    }
    // 由 this.run/onTrack/forceDirtyDepsRecompute 调用
    // CAUTION 原型方法 + 惰性 bound 版本（scheduleRecompute 需要脱离 this 调用）
    _boundRecompute?: (forceRecompute?: boolean) => void
    get boundRecompute() {
        return this._boundRecompute ?? (this._boundRecompute = (forceRecompute?: boolean) => this.recomputeInternal(forceRecompute))
    }
    // 内部触发路径（handleTriggered/调度器）用的重算：不经过 cleanPromise 的创建性 getter。
    // CAUTION 内部路径不能创建 cleanPromise：内部调用方从不 await，无谓地创建会让
    //  handleRecomputeError 误以为"有等待方在观测错误"，把本应 console.error 兜底的
    //  async 错误静默吞掉。只有外部调用 recompute()/读取 cleanPromise 才创建。
    // CAUTION 不能声明为 async：同步 computed 的重算（以及其中的用户异常）必须保持同步语义。
    recomputeInternal(forceRecompute = false) {
        // CAUTION !this.getter：源模式结构/占位 Computed 没有计算可言。它们现在
        //  active === true（生命周期可销毁），不能再依赖 active 来挡住重算路径。
        if (!this.getter || (this._status === STATUS_CLEAN && !forceRecompute) || !this.active) return

        // 四种类型计算：
        // async/sync * full/patchable

        // 这三种情况需要开启新的 fullRecompute。
        // 1. 外部强制的 recompute
        // 2. full recompute 模式
        // 3. patchable recompute 的 initial 状态
        // 剩下就只有 patchable recompute 的 patch 阶段了

        // 非 async 的计算不会被打断，都是一次性就执行完了。
        // 1. forceRecompute 会打断所有的 async 的计算。
        // 2. async full recompute 自己会打断上一次的
        // 3. async patchable
        // 3.1. 在计算过程中就不需要管了
        // 3.2. 不在就开启新的 patch 计算。

        const needFullRecompute = forceRecompute|| !this.applyPatch || this.phase === FULL_RECOMPUTE_PHASE

        if (needFullRecompute) {
            this.fullRecompute()
        } else {
            if (!this.inPatch) {
                this.patchRecompute()
            }
        }
    }
    // 外部 API：返回 cleanPromise 供调用方 await 本轮计算完成（同步完成时返回 undefined）。
    recompute(forceRecompute = false): Promise<any> | undefined {
        this.recomputeInternal(forceRecompute)
        return this.cleanPromise
    }
    runPatch() {
        if (this.isAsyncPatch || this.isGeneratorPatch) {
            return this.isAsyncPatch ? this.runAsyncPatch() : this.runGeneratorPatch()
        } else {
            return this.runSimplePatch()
        }
    }
    runSimplePatch() {
        this.prepareTracking(false, true)
        this.pauseAutoTrack()
        // CAUTION try/finally 保证用户 applyPatch 抛异常时追踪状态一定复位
        try {
            return (this.applyPatch as SimpleApplyPatchType<any>).call(this, this.data, this.triggerInfos)
        } finally {
            this.resetAutoTrack()
            this.completeTracking(false, true)
            this.triggerInfos.length = 0
        }
    }
    async runAsyncPatch() {
        let patchResult
        // CAUTION 每轮循环（以及每个 await 恢复点）都要检查 active：destroy 不会中止
        //  已挂起的 applyPatch，但必须阻止其结果被继续应用/续跑（否则已销毁实例的
        //  data 会被在途 patch 复活改写）。
        while(this.active && this.triggerInfos.length) {
            const waitingTriggerInfos = [...this.triggerInfos]
            this.triggerInfos.length = 0
            let patchPromise
            // CAUTION scope 的 push/pop 只包住 applyPatch 的同步段（返回 promise 之前）：
            //  旧实现让本 effect 在整个 async patch（跨所有 await）期间都留在
            //  activeScopes 顶上，造成三个后果——
            //  1. await 挂起期间任何无关代码读 atom 都被追踪成本 effect 的依赖（幽灵订阅）；
            //  2. 挂起期间源的新写入因 trigger 的 activeEffect 抑制被静默丢弃（数据永久丢失）；
            //  3. 两个 async patch 交错完成时 completeTracking 的 pop() 弹掉的是对方。
            //  与 async getter 一致，patch 的依赖归属只覆盖第一个 await 之前的同步段。
            this.prepareTracking(false, true)
            this.pauseAutoTrack()
            try {
                patchPromise = (this.applyPatch as AsyncApplyPatchType<any>).call(this, this.data, waitingTriggerInfos)
            } finally {
                this.resetAutoTrack()
                this.completeTracking(false, true)
            }
            patchResult = await patchPromise
            if (patchResult === false) {
                break
            }
        }
        return patchResult
    }
    async runGeneratorPatch() {
        let patchResult

        // active 检查与 runAsyncPatch 相同：destroy 后不再续跑
        while(this.active && this.triggerInfos.length) {
            const waitingTriggerInfos = [...this.triggerInfos]
            this.triggerInfos.length =0
            const generator = (this.applyPatch! as GeneratorApplyPatchType<any>).call(this, this.data, waitingTriggerInfos)

            this.asyncStatus!(true)
            try {
                patchResult = await this.runGenerator(generator,
                    (isFirst) => {
                        this.prepareTracking(false, true)
                        this.pauseAutoTrack()
                    },
                    (isLast) => {
                        this.resetAutoTrack()
                        this.completeTracking(false, true)
                    }
                )
            } finally {
                this.asyncStatus!(false)
            }
        }

        return patchResult
    }
    public lastCleanupFn?: () => void

    // rxList/rxMap 必须覆写
    replaceData(newData: any) {

    }

    hasDeps() {
        return this.deps.length > 0
    }
    // 给继承者在 apply catch 中用的 工具函数
    // CAUTION 以下全部是原型方法（原来是每实例的箭头函数字段），调用点都是 this.xxx() 的方法调用
    manualTrack(target: object, type: TrackOpTypes, key: unknown) {
        notifier.enableTracking()
        // CAUTION try/finally 配平：track 会向监听者派发 track 事件（用户代码），
        //  抛错时 trackStack 必须复原,否则全局追踪开关永久失衡。
        try {
            return isPrimitiveAtom(target) && type === TrackOpTypes.ATOM && key === 'value'
                ? notifier.trackPrimitiveAtomValue(target)
                : notifier.track(target, type, key)
        } finally {
            notifier.resetTracking()
        }
    }
    pauseAutoTrack() {
        notifier.pauseTracking()
    }
    autoTrack() {
        notifier.enableTracking()
    }
    resetAutoTrack() {
        notifier.resetTracking()
    }
    /**
     * @internal
     * 由静态 ReactiveEffect.destroy 核心调用（见基类说明）：无论销毁入口是实例
     * destroy()、父 effect 的 destroyChildren 还是 destroyComputed，都恰好执行一次。
     */
    destroyResources() {
        // CAUTION 推进 recomputeId 使一切在途 async fullRecompute/patch 的收尾分支
        //  （recomputeId 比对）失效；配合 runAsyncPatch/runGeneratorPatch 的 active
        //  检查与 Rx 结构变更方法的 active 守卫，destroy 之后在途 patch 不再改写数据。
        this.recomputeId++
        this.lastCleanupFn?.()
        delete this.lastCleanupFn
        // 计算尚未完成就被销毁时，解除所有 await cleanPromise 的等待方
        this.settleCleanPromise()
        super.destroyResources()
    }
    collectEffect(): () => CleanupFrame {
        return ReactiveEffect.collectEffect()
    }
    destroyEffect(effect: ReactiveEffect) {
        // 因为可能是 computed，destroy 和 ReactiveEffect 不一样，所以要调用它自己身的
        effect.destroy()
    }
    _cachedValues?: Map<any, any>
    get cachedValues(): Map<any, any> {
        return this._cachedValues ?? (this._cachedValues = new Map())
    }
    getCachedValue<T>(effect:any, createFn: () => T) : T{
        // CAUTION 用 has 判断命中：createFn 可能返回 0/''/false/null 等 falsy 值，
        //  用真值判断会导致这类值每次都重建。
        const cached = this._cachedValues
        if (cached?.has(effect)) return cached.get(effect)
        const value = createFn()
        this.cachedValues.set(effect, value)
        return value
    }
}

// 恒定默认值放在原型上：实例只在真正改写时才产生自有属性（同 ReactiveEffect 的做法）
Computed.prototype.scheduleNeedsInfos = false
Computed.prototype._cleanExpected = false
Computed.prototype.preventEffectSession = false

/**
 * @category Basic
 */
export function computed<T>(getter: GetterType<T>, applyPatch?: ApplyPatchType<T>, dirtyCallback?: DirtyCallback | true, callbacks?: CallbacksType, skipIndicator?: SkipIndicator) {
    const internal = new AtomComputed(getter, applyPatch, dirtyCallback, callbacks, skipIndicator)
    computedToInternal.set(internal.data, internal)
    return internal.data as Atom<T>
}

export class AtomComputed extends Computed{
    constructor(getter?: GetterType, applyPatch?: ApplyPatchType, dirtyCallback?: DirtyCallback|true, callbacks?: CallbacksType, skipIndicator?: SkipIndicator) {
        super(getter, applyPatch, dirtyCallback, callbacks, skipIndicator)
        this.data = atom(null)
        // 无 getter 的占位实例（reduce 的 placeholder 等）没有计算可跑
        if (getter) {
            this.run([], true)
        }
    }
    replaceData(newData: any) {
        if(isAtom(newData)) {
            this.data(newData.raw)
        } else {
            this.data(newData)
        }
    }
}

// 强制重算。返回 cleanPromise，async computed 的调用方可以 await 本轮计算完成。
export function recompute(computedItem: ComputedData, force = false) {
    const internal = computedToInternal.get(computedItem)!
    return internal.recompute(force)
}

// 目前 debug 用的
export function isComputed(target: any) {
    return !!computedToInternal.get(target)
}

// debug 时用的
export function getComputedGetter(target: any) {
    return computedToInternal.get(target)?.getter
}

