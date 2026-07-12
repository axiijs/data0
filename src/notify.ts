import {CompactDep, createCompactDep, createDep, Dep, newTracked, wasTracked} from "./dep";
import {TrackOpTypes, TriggerOpTypes} from "./operations";
import {Computed} from "./computed";
import {assert, extend} from "./util";
import {ReactiveEffect} from "./reactiveEffect.js";
import {
  trackRetainedDepEffectAdded,
  trackRetainedPrimitiveAtomDepCreated
} from "./retainedDiagnostics";


type KeyItemPair = {
  key?: any,
  oldValue?: any
  newValue?: any
}
export type TriggerResult = {
  add?: KeyItemPair[]
  update?: KeyItemPair[]
  remove?: KeyItemPair[]
}
type KeyToDepMap = Map<any, Dep>
const PRIMITIVE_ATOM_DEP = Symbol('primitive atom dep')
type PrimitiveAtomDepTarget = object & {
  [PRIMITIVE_ATOM_DEP]?: Dep
}

export type TriggerStack = {type?: string, debugTarget: any, opType?: TriggerOpTypes, key?:unknown, oldValue?: unknown, newValue?: unknown, targetLoc: [string, string][]}[]
export type InputTriggerInfo<T = unknown> = {
  method?: string,
  argv?: any[]
  result? : TriggerResult,
  methodResult? :any
  reorderInfo?: unknown,
  key?: unknown,
  newValue?: T,
  oldValue?: T,
}

export type TriggerInfo<T = unknown> = {
  type: TriggerOpTypes,
  source: any
} & InputTriggerInfo<T>

export type DebuggerEventExtraInfo = {
  target: object
  type: TrackOpTypes | TriggerOpTypes
  key: any
  newValue?: any
  oldValue?: any
  oldTarget?: Map<any, any> | Set<any>
}
export type DebuggerEvent = {
  effect: ReactiveEffect
} & DebuggerEventExtraInfo



// Map/Set 或者自定义结构 iterator 的时候用到。
// CAUTION array/object 不用，因为他们迭代的时候会触发具体 key 的 track。
export const ITERATE_KEY = Symbol( 'iterate' )
// Object/Array 执行 ownKeys 或者 Map 执行 keys 的时候用到。
export const ITERATE_KEY_KEY_ONLY = Symbol('Map key iterate' )
/**
 * The bitwise track markers support at most 30 levels of recursion.
 * This value is chosen to enable modern JS engines to use a SMI on all platforms.
 * When recursion depth is greater, fall back to using a full cleanup.
 * @internal
 */
export const maxMarkerBits = 30


export class Notifier {
  static trackOpBit  = 1
  static _instance: Notifier
  static get instance() {
    return Notifier._instance || (Notifier._instance = new Notifier())
  }
  trackTargetFrames: any[][] = []
  // 栈顶 frame 的直接引用：track 热路径上每次读 trackTargetFrames.at(-1) 是一次
  // 通用方法调用，绝大多数时候栈是空的，用一个字段判空即可
  currentTrackFrame?: any[]
  // 被 track 的对象 {target -> key -> dep}
  targetMap= new WeakMap<any, KeyToDepMap>()
  shouldTrack = true
  effectTrackDepth = 0
  shouldTrigger: boolean = true
  trackStack: boolean[] = []
  // session（batch）队列：去重标记与待处理 info 放在 effect 实例字段上
  // （_inSession/_sessionInfos），代替原来的 Set + WeakMap——batch 内每次触发
  // 少一次哈希查找，digest 时顺序数组遍历也比 Set 迭代快。
  sessionQueue: ReactiveEffect[] = []
  inEffectSession: boolean = false
  isDigesting: boolean = false
  sessionDepth = 0
  createEffectSession() {
    if (this.isDigesting) return
    this.inEffectSession = true
    this.sessionDepth++
  }
  scheduleEffect(effect: ReactiveEffect, source: any, type: TriggerOpTypes, inputInfo?: InputTriggerInfo) {
    if (__DEV__) {
      assert(this.inEffectSession, 'should be in effect session')
    }
    if (!effect._inSession) {
      effect._inSession = true
      this.sessionQueue.push(effect)
    }
    if (effect.needsTriggerInfo) {
      (effect._sessionInfos ?? (effect._sessionInfos = [])).push(
          (inputInfo ? {...inputInfo, source, type} : {source, type}) as TriggerInfo
      )
    }
  }
  scheduleAtomEffect(effect: ReactiveEffect, source: any, newValue?: unknown, oldValue?: unknown) {
    if (!effect._inSession) {
      effect._inSession = true
      this.sessionQueue.push(effect)
    }
    if (effect.needsTriggerInfo) {
      (effect._sessionInfos ?? (effect._sessionInfos = [])).push(
          {source, type: TriggerOpTypes.ATOM, key: 'value', newValue, oldValue} as TriggerInfo
      )
    }
  }
  digestEffectSession() {
    if (this.isDigesting) return
    this.sessionDepth--
    if (this.sessionDepth > 0) return

    const queue = this.sessionQueue
    // 空 session 快出口：同步 computed 每次 finishFullRecompute 都会建立/消化一次 session
    if (queue.length === 0) {
      this.inEffectSession = false
      return
    }

    this.isDigesting = true
    // CAUTION 单个 effect 抛异常不能中断 digest：旧实现直接丢弃队列中尚未执行的
    //  effect——它们的"标脏"发生在 run 里，被丢弃后 status 停留在 CLEAN，读到的是
    //  静默的陈旧值（batch 中一个订阅者出错会污染其他所有订阅者的数据一致性）。
    //  现在逐个 try/catch，保证所有 effect 都执行；第一个错误在 digest 完成后
    //  重新抛给 batch 调用方，其余错误 console.error 上报（不静默吞掉）。
    let hasError = false
    let firstError: unknown
    try {
        // CAUTION queue.length 每轮重新读取：digest 过程中新触发（含重入）的 effect
        //  会追加到队尾，在同一次 digest 中被处理（与旧的 Set 迭代语义一致）。
        for (let i = 0; i < queue.length; i++) {
            const effect = queue[i]
            effect._inSession = false
            const infos = effect._sessionInfos
            if (infos !== undefined) effect._sessionInfos = undefined
            try {
                if (infos !== undefined) {
                    effect.run(infos)
                } else {
                    effect.run()
                }
            } catch (err) {
                if (hasError) {
                    console.error('[data0] suppressed additional effect error in batch digest:', err)
                } else {
                    hasError = true
                    firstError = err
                }
            }
        }
    } finally {
        // 防御：即使出现预期外的异常（如 OOM），也要复位排队项标记与 session 状态，
        // 防止 notifier 永久卡在 session 中，后续所有 trigger 都被吞掉。
        // （正常路径下这些项都已处理过，重复复位是幂等的。）
        for (let j = 0; j < queue.length; j++) {
            queue[j]._inSession = false
            queue[j]._sessionInfos = undefined
        }
        queue.length = 0
        this.inEffectSession = false
        this.isDigesting = false
    }
    if (__DEV__) {
        // dev 不变量：digest 退出后 session 状态必须完全静止（防未来编辑破坏 finally 语义）
        assert(this.sessionQueue.length === 0 && !this.inEffectSession && !this.isDigesting && this.sessionDepth === 0,
            'effect session state not quiescent after digest')
    }
    if (hasError) throw firstError
  }
  collectTrackTarget() {
    const frame:any[] = []
    this.trackTargetFrames.push(frame)
    this.currentTrackFrame = frame
    return () => {
      assert(frame === this.currentTrackFrame, 'track target frame error.')
      // CAUTION 必须弹栈，否则 frame 永久驻留并继续收集后续所有 track target（内存泄漏）。
      const frames = this.trackTargetFrames
      frames.pop()
      this.currentTrackFrame = frames.length ? frames[frames.length - 1] : undefined
      return frame
    }
  }
  getPrimitiveAtomDep(target: object) {
    return (target as PrimitiveAtomDepTarget)[PRIMITIVE_ATOM_DEP]
  }
  getOrCreatePrimitiveAtomDep(target: object) {
    let dep = this.getPrimitiveAtomDep(target)
    if (!dep) {
      // CAUTION 普通赋值而不是 defineProperty：defineProperty 会把 atom 函数对象推进
      //  字典属性模式（每个被订阅的 atom 多 ~200B）。symbol key 不会污染 for...in/Object.keys。
      ;(target as PrimitiveAtomDepTarget)[PRIMITIVE_ATOM_DEP] = dep = createCompactDep()
      trackRetainedPrimitiveAtomDepCreated(dep)
    }
    return dep
  }
  trackPrimitiveAtomValue = (target: object) => {
    const scopes = ReactiveEffect.activeScopes
    const activeEffect = scopes.length ? scopes[scopes.length - 1] : undefined
    if (!activeEffect || !this.shouldTrack) return
    if (__DEV__) {
      assert(!(activeEffect instanceof Computed && target === activeEffect.data), 'should not read self in computed')
    }

    const dep = this.getOrCreatePrimitiveAtomDep(target)
    const eventInfo = __DEV__
        ? { effect: activeEffect, target, type: TrackOpTypes.ATOM, key: 'value' }
        : undefined

    // 手动收集的场景。
    if (this.currentTrackFrame !== undefined) this.currentTrackFrame.push(target)

    this.trackEffects(dep, eventInfo)
    return dep
  }
  track = (target: object, type: TrackOpTypes, key: unknown) => {
    const scopes = ReactiveEffect.activeScopes
    const activeEffect = scopes.length ? scopes[scopes.length - 1] : undefined
    if (!activeEffect || !this.shouldTrack) return
    // CAUTION 不能 track 自己。computed 在第二次执行的时候会有一个 replace 行为，会
    if (__DEV__) {
      assert(!(activeEffect instanceof Computed && target === activeEffect.data), 'should not read self in computed')
    }

    // FIXME 对 async 的 reactive，要暂存，complete 的时候才确认。因为它是可以被打断重算的。

    let depsMap = this.targetMap.get(target)
    if (!depsMap) {
      this.targetMap.set(target, (depsMap = new Map()))
    }
    let dep = depsMap.get(key)
    if (!dep) {
      depsMap.set(key, (dep = createDep()))
    }

    const eventInfo = __DEV__
        ? { effect: activeEffect, target, type, key }
        : undefined

    // 手动收集的场景。
    if (this.currentTrackFrame !== undefined) this.currentTrackFrame.push(target)

    this.trackEffects(dep, eventInfo)
    return dep
  }
  trackEffects(
      dep: Dep,
      debuggerEventExtraInfo?: DebuggerEventExtraInfo
  ) {
    const scopes = ReactiveEffect.activeScopes
    const activeEffect = scopes.length ? scopes[scopes.length - 1] : undefined
    if (!activeEffect) return
    let shouldTrack = false
    if (!activeEffect.isAsync) {
      if (activeEffect.useDepMarker && this.effectTrackDepth <= maxMarkerBits) {
        if (!newTracked(dep)) {
          dep.n |= Notifier.trackOpBit // set newly tracked
          shouldTrack = !wasTracked(dep)
        }
      } else {
        // Full cleanup mode.
        shouldTrack = !dep.has(activeEffect!)
      }
    } else {
      // async 模式，因为最终是用延迟的 track 来覆盖，所以总是应该 track
      shouldTrack = true
    }

    if (shouldTrack) {
      // CAUTION 即使是 async 的模式，也应该变 run 边 track 新的。
      //  这样不管是因为老的 dep 变化，还是新  track 到一半的 dep 变化，都会触发 recompute。
      //  这才是合理的，因为不管哪种都说明 dirty。
      dep.add(activeEffect!)
      trackRetainedDepEffectAdded(dep)
      activeEffect!.addDep(dep)
      // 如果是 async 的任务，那么在最后 complete 的时候应该应该用新的 dep 完全替换旧的 dep
      if (activeEffect.isAsync) {
        activeEffect.queueAsyncTrack(() => {
          if(!dep.has(activeEffect!)) {
            dep.add(activeEffect!)
            trackRetainedDepEffectAdded(dep)
            activeEffect!.addDep(dep)
          }
        })
      }

      // CAUTION 先判断监听者再构造 payload：track 是最热的路径之一，
      //  无监听者时不能为派发分配对象。
      if (activeEffect.hasListener('track')) {
        activeEffect.dispatch('track', {
          effect: activeEffect,
          ...debuggerEventExtraInfo
        })
      }
    }
  }
  trigger(
      source: object,
      type: TriggerOpTypes,
      inputInfo: InputTriggerInfo,
      oldTarget?: Map<unknown, unknown> | Set<unknown>
  ) {
    if (!this.shouldTrigger) return

    // CAUTION 这里不再预先构造 TriggerInfo（{...inputInfo, source, type}）：
    //  只有 patch 型 Computed 才消费 info，由 runFromTrigger/scheduleEffect 按需组装。
    const {key, newValue, oldValue} = inputInfo
    const depsMap = this.targetMap.get(source)
    if (!depsMap) {
      // never been tracked
      return
    }

    let deps: (Dep | undefined)[] = []
    if (type === TriggerOpTypes.CLEAR) {
      // collection being cleared
      // trigger all effects for target
      deps = [...depsMap.values()]

    } else {
      // schedule runs for SET | ADD | DELETE
      if (key !== void 0) {
        deps.push(depsMap.get(key))
      }

      // also run for iteration key on ADD | DELETE | Map.SET
      // CAUTION 2026-H2 删除了 Vue 遗留的 raw-array 分支（key==='length' 收缩触发、
      //  ADD 整数 key 触发 'length' dep）：data0 没有数组 Proxy，所有内部触发源都是
      //  Rx 类实例或 atom（isArray(source) 恒 false），该分支仅被覆盖率测试喂养。
      //  以 raw array 为 trigger target 不属于承诺面（README 只承诺 Rx 结构与 atom）。
      switch (type) {
        case TriggerOpTypes.ADD:
        case TriggerOpTypes.DELETE:
          deps.push(depsMap.get(ITERATE_KEY))
          deps.push(depsMap.get(ITERATE_KEY_KEY_ONLY))
          break
        case TriggerOpTypes.SET:
          deps.push(depsMap.get(ITERATE_KEY))
          break
        case TriggerOpTypes.METHOD:
          deps.push(depsMap.get(TriggerOpTypes.METHOD))
          break
        case TriggerOpTypes.EXPLICIT_KEY_CHANGE:
          deps.push(depsMap.get(TriggerOpTypes.EXPLICIT_KEY_CHANGE))
          break
      }
    }

    const eventInfo = __DEV__
        ? { target: source, type, key, newValue, oldValue, oldTarget }
        : undefined

    // 找出非空 dep：绝大多数 trigger 只命中一个 dep，走无去重的直接派发；
    // 多个 dep 时同一个 effect 可能同时订阅了 key dep 和 ITERATE dep，必须去重
    // （否则 patch computed 会收到重复 info），并且要先快照再执行。
    let onlyDep: Dep | undefined
    let hasMultiple = false
    for (const dep of deps) {
      if (!dep) continue
      if (onlyDep === undefined) {
        onlyDep = dep
      } else {
        hasMultiple = true
        break
      }
    }
    if (onlyDep === undefined) return

    if (!hasMultiple) {
      this.triggerEffects(onlyDep, source, type, inputInfo, eventInfo)
    } else {
      const dedupedEffects = new Set<ReactiveEffect>()
      for (const dep of deps) {
        if (!dep) continue
        for (const effect of dep) {
          dedupedEffects.add(effect)
        }
      }
      const scopes = ReactiveEffect.activeScopes
      const activeEffect = scopes.length ? scopes[scopes.length - 1] : undefined
      for (const effect of dedupedEffects) {
        if (effect !== activeEffect) {
          this.triggerEffect(effect, source, type, inputInfo, eventInfo)
        }
      }
    }
  }
  // primitive atom 写入的特化路径：newValue/oldValue 以标量传递，
  // 无订阅者/轻量订阅者（不消费 info 的 effect）时全程零对象分配。
  triggerPrimitiveAtomValue(
      source: object,
      newValue?: unknown,
      oldValue?: unknown
  ) {
    if (!this.shouldTrigger) return

    const dep = this.getPrimitiveAtomDep(source) as CompactDep | undefined
    if (!dep) return

    const eventInfo = __DEV__
        ? { target: source, type: TriggerOpTypes.ATOM, key: 'value', newValue, oldValue }
        : undefined

    const scopes = ReactiveEffect.activeScopes
    const activeEffect = scopes.length ? scopes[scopes.length - 1] : undefined
    // CompactDep 单订阅者快路径：这是渲染框架里最热的形态（一个 atom 一个绑定 effect）
    const single = dep.single
    if (single !== undefined) {
      if (single !== activeEffect) {
        this.triggerAtomEffect(single, source, newValue, oldValue, eventInfo)
      }
      return
    }
    if (dep.overflow === undefined) return
    // CAUTION 快照稳定化：effect 执行中可能增删订阅（对 native Set 展开，比 generator 快）
    const effects = [...dep.overflow]
    for (const effect of effects) {
      if (effect !== activeEffect) {
        this.triggerAtomEffect(effect, source, newValue, oldValue, eventInfo)
      }
    }
  }
  triggerAtomEffect(
      effect: ReactiveEffect,
      source: any,
      newValue?: unknown,
      oldValue?: unknown,
      debuggerEventExtraInfo?: DebuggerEventExtraInfo
  ) {
    if (effect.hasListener('trigger')) {
      effect.dispatch('trigger', extend({ effect }, debuggerEventExtraInfo))
    }
    if (this.inEffectSession) {
      this.scheduleAtomEffect(effect, source, newValue, oldValue)
    } else {
      effect.runFromAtomTrigger(source, newValue, oldValue)
    }
  }
  getDepEffects(target: object) {
    const depsMap = this.targetMap.get(target)
    const primitiveAtomDep = this.getPrimitiveAtomDep(target)
    if (!depsMap && !primitiveAtomDep) return

    // CAUTION 一定要利用 set 去重，不然外部拿到的结果可能引发问题。
    const result = new Set<ReactiveEffect>()
    if (primitiveAtomDep) {
      for(const effect of primitiveAtomDep) {
        result.add(effect)
      }
    }
    if (depsMap) {
      for(const [_, deps] of depsMap) {
        for(const effect of deps) {
          result.add(effect)
        }
      }
    }
    return result
  }
  // CAUTION 架构语义（AGENTS.md「架构决策与已知语义边界」A1）：传播是急切推模式，
  //  按订阅顺序同步执行，无拓扑排序、无读时拉取。菱形依赖（a→c 且 a→b→c）下，
  //  先订阅的下游会以"新 a + 旧 b"先算一遍（可观察的中间值 + 重复重算），终值必然
  //  收敛。这不是缺陷：glitch-free 需要拉模式/拓扑调度，与当前架构冲突，明确不修。
  triggerEffects(
      dep: Dep,
      source: any,
      type: TriggerOpTypes,
      inputInfo?: InputTriggerInfo,
      debuggerEventExtraInfo?: DebuggerEventExtraInfo
  ) {
    const scopes = ReactiveEffect.activeScopes
    const activeEffect = scopes.length ? scopes[scopes.length - 1] : undefined

    // CompactDep 单订阅者快路径：零迭代器、零数组
    if (dep instanceof CompactDep) {
      const single = dep.single
      if (single !== undefined) {
        // CAUTION 特别注意这里，因为我们现在支持了 lazy recompute，所以可能在读的时候才重算。
        //  重算过程中可能会再次出发 trigger，因为像 atomComputed 这种是在重算的时候更新 atom 值的。
        if (single !== activeEffect) {
          this.triggerEffect(single, source, type, inputInfo, debuggerEventExtraInfo)
        }
        return
      }
      if (dep.overflow === undefined) return
      const effects = [...dep.overflow]
      for (const effect of effects) {
        if (effect !== activeEffect) {
          this.triggerEffect(effect, source, type, inputInfo, debuggerEventExtraInfo)
        }
      }
      return
    }

    // CAUTION 快照稳定化：effect 执行过程中可能向 dep 增删订阅，
    //  不能直接在 live Set 上迭代（新增的订阅会被本轮误触发）。
    const effects = [...(dep as unknown as Set<ReactiveEffect>)]
    for (const effect of effects) {
      if (effect !== activeEffect) {
        this.triggerEffect(effect, source, type, inputInfo, debuggerEventExtraInfo)
      }
    }
  }
  triggerEffect(
      effect: ReactiveEffect,
      source: any,
      type: TriggerOpTypes,
      inputInfo?: InputTriggerInfo,
      debuggerEventExtraInfo?: DebuggerEventExtraInfo
  ) {
    if (__DEV__) {
      const scopes = ReactiveEffect.activeScopes
      assert((scopes.length ? scopes[scopes.length - 1] : undefined) !== effect, 'recursive effect call')
    }

    // 无监听者时不构造 payload（每次触发每个 effect 都会经过这里）
    if (effect.hasListener('trigger')) {
      effect.dispatch('trigger', extend({ effect }, debuggerEventExtraInfo))
    }

    if (this.inEffectSession) {
      this.scheduleEffect(effect, source, type, inputInfo)
    } else {
      effect.runFromTrigger(source, type, inputInfo)
    }
  }
  enableTracking() {
    this.trackStack.push(this.shouldTrack)
    this.shouldTrack = true
  }
  pauseTracking() {
    this.trackStack.push(this.shouldTrack)
    this.shouldTrack = false
  }
  resetTracking() {
    const last = this.trackStack.pop()
    this.shouldTrack = last === undefined ? true : last
  }
}

// CAUTION 模块级单例引用：热路径（atom 读写、effect run、RxList track）每次经过
//  Notifier.instance 静态 getter 都要做一次 `_instance ||` 判断。这里在模块求值时
//  创建好单例，内部热点直接引用。Notifier.instance 公开 API 不变（返回同一个实例）。
//  注意放在类定义之后求值；循环依赖模块（computed/reactiveEffect）只在函数体内使用，
//  ESM live binding 保证调用时已初始化。
export const notifier: Notifier = Notifier.instance

/**
 * Batch reactive writes so dependent effects run once after the callback exits.
 *
 * Nested batches share the same effect session and flush only at the outermost
 * boundary.
 *
 * CAUTION 架构语义（AGENTS.md「架构决策与已知语义边界」A2）：session 把订阅者的
 *  执行连同"标脏"一起推迟到 digest，而 computed 读路径没有"脏则重算"的拉取。
 *  因此 batch 内"先写依赖、再读该依赖的 computed"读到的是进入 batch 前的旧值
 *  （atom 本身的读取不受影响），batch 退出后恢复一致。这不是缺陷，明确不修。
 */
export function batch<T>(fn: () => T): T {
  // dev 不变量：batch 结束后全局作用域栈/追踪开关栈深度必须复原。
  // 违约（某个订阅者泄漏了 scope 或 pause/reset 不配平）在边界当场炸。
  let scopesDepthBefore = 0
  let trackStackDepthBefore = 0
  if (__DEV__) {
    scopesDepthBefore = ReactiveEffect.activeScopes.length
    trackStackDepthBefore = notifier.trackStack.length
  }
  notifier.createEffectSession()
  try {
    return fn()
  } finally {
    notifier.digestEffectSession()
    if (__DEV__) {
      assert(ReactiveEffect.activeScopes.length === scopesDepthBefore, 'activeScopes depth not restored after batch (scope leak)')
      assert(notifier.trackStack.length === trackStackDepthBefore, 'trackStack depth not restored after batch (pause/reset imbalance)')
    }
  }
}
