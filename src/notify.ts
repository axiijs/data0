import {CompactDep, createCompactDep, createDep, Dep, newTracked, reattachDepToHost, wasTracked} from "./dep";
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
// CAUTION 协议订阅（METHOD/EXPLICIT_KEY_CHANGE）的 depsMap 记账 key。
//  曾直接用字符串枚举值（'method'/'explicit_key_change'）作 key，与用户数据 key
//  共享同一命名空间：RxMap 的 key / RxSet 的成员 / groupBy 的组键恰为这两个字符串时
//  （按 HTTP method 分组是完全现实的输入），SET/ADD/DELETE 的 key dep 与内部
//  METHOD 订阅者是同一个 dep——派生结构的 applyPatch 收到非协议形状的 info：
//  RxMap.keys 的 assert(unreachable) 直接抛给 map.set 调用方，RxSet.toList 等
//  对 methodResult 解构 TypeError 且派生静默分叉。track/trigger 双侧统一经
//  normalizeTrackKey 映射为内部 Symbol，用户 key 与协议 key 从构造上隔离
//  （公开调用形状 manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD) 不变）。
export const METHOD_TRACK_KEY = Symbol('method track key')
export const EXPLICIT_KEY_CHANGE_TRACK_KEY = Symbol('explicit key change track key')
// CAUTION 先比 type 再比 key：track 是最热路径之一，绝大多数调用 type 是
//  'get'/'atom'/'iterate'，一次 interned 字符串比较（指针比较）即可短路。
function normalizeTrackKey(type: TrackOpTypes, key: unknown): unknown {
  if (type === TrackOpTypes.METHOD) {
    if (key === TriggerOpTypes.METHOD) return METHOD_TRACK_KEY
  } else if (type === TrackOpTypes.EXPLICIT_KEY_CHANGE) {
    if (key === TriggerOpTypes.EXPLICIT_KEY_CHANGE) return EXPLICIT_KEY_CHANGE_TRACK_KEY
  }
  return key
}
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
    // CAUTION 循环诊断阈值(2026-H3 round8 R8-8,仅 dev):非 batch 下互触发环会被
    //  "detect recompute in sync recompute" 断言当场喝止,batch 的 digest 队列却
    //  允许无界追加(重入合法:级联触发是正常语义)——真正非收敛的环在 batch 内
    //  是**静默无限循环**(挂死无任何信号),同一形态在两个入口一 loud 一 silent。
    //  死循环本身无法用测试表达(先有界才可断言),这里在处理量越过
    //  「max(初始队列 × 16, 100_000)」时 console.error 一次(不中断、不改语义:
    //  合法的大 batch 初始队列可以很大,倍数 + 绝对量双门避免误报),把挂死变成
    //  可诊断。生产构建零开销。
    const digestCycleWarnAt = __DEV__ ? Math.max(queue.length * 16, 100_000) : 0
    try {
        // CAUTION queue.length 每轮重新读取：digest 过程中新触发（含重入）的 effect
        //  会追加到队尾，在同一次 digest 中被处理（与旧的 Set 迭代语义一致）。
        for (let i = 0; i < queue.length; i++) {
            if (__DEV__ && i === digestCycleWarnAt) {
                console.error(`[data0] batch digest has processed ${i} effects and the queue is still growing — `
                    + `likely a non-converging effect cycle (subscribers re-triggering each other endlessly). `
                    + `The digest will keep running; if this hangs, break the cycle or move the writes out of the subscribers.`)
            }
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

    // 协议 key（METHOD/EXPLICIT_KEY_CHANGE）与用户数据 key 的命名空间隔离
    key = normalizeTrackKey(type, key)

    let depsMap = this.targetMap.get(target)
    if (!depsMap) {
      this.targetMap.set(target, (depsMap = new Map()))
    }
    let dep = depsMap.get(key)
    if (!dep) {
      depsMap.set(key, (dep = createDep()))
      // 宿主记账：退订到空时把本 dep 从 depsMap 摘除（见 dep.ts pruneEmptyDepFromHost），
      // 否则"订阅不同 key → 退订"的循环会在长活 target 上留下无界的空 Dep 条目。
      dep.host = depsMap
      dep.hostKey = key
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
            // async 收尾先 cleanup 再重放 asyncTracks：cleanup 可能把瞬时清空的
            // dep 从宿主摘除（pruneEmptyDepFromHost），这里挂回，否则重订阅落在
            // 孤儿 dep 上，后续 trigger 永远找不到本 effect。
            const effective = reattachDepToHost(dep, activeEffect!)
            if (effective !== dep) activeEffect!.addDep(effective)
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
      // CAUTION 库内无生产触发方(RxMap.clear 走逐 key DELETE + METHOD):本分支
      //  是留给「手动 notifier.trigger 的自定义结构」(LinkedList 式用法)的协议面,
      //  枚举成员是公开 API,勿当死代码删除(2026-H3 round6 工程面清偿时裁定保留)。
      deps = [...depsMap.values()]

    } else {
      // schedule runs for SET | ADD | DELETE
      // CAUTION 'key' in inputInfo 兜住"key 恰为 undefined"的带内值：RxMap 支持
      //  undefined 作为合法 key，get(undefined) 的订阅者曾因 `key !== void 0`
      //  把"未提供 key"与"key 为 undefined"混为一谈而永久漏触发（静默陈旧）。
      if (key !== void 0 || 'key' in inputInfo) {
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
          deps.push(depsMap.get(METHOD_TRACK_KEY))
          break
        case TriggerOpTypes.EXPLICIT_KEY_CHANGE:
          deps.push(depsMap.get(EXPLICIT_KEY_CHANGE_TRACK_KEY))
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
      // 错误隔离与 triggerEffects 一致（round9 F1，说明见该方法）
      let hasError = false
      let firstError: unknown
      for (const effect of dedupedEffects) {
        if (effect !== activeEffect) {
          try {
            this.triggerEffect(effect, source, type, inputInfo, eventInfo)
          } catch (err) {
            if (hasError) {
              Notifier.reportSuppressedInlineError(err)
            } else {
              hasError = true
              firstError = err
            }
          }
        }
      }
      if (hasError) throw firstError
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
    // 错误隔离与 triggerEffects 一致（round9 F1，说明见该方法）；single 快路径
    // 无兄弟订阅者可保护，异常直接透传，保持零开销。
    const effects = [...dep.overflow]
    let hasError = false
    let firstError: unknown
    for (const effect of effects) {
      if (effect !== activeEffect) {
        try {
          this.triggerAtomEffect(effect, source, newValue, oldValue, eventInfo)
        } catch (err) {
          if (hasError) {
            Notifier.reportSuppressedInlineError(err)
          } else {
            hasError = true
            firstError = err
          }
        }
      }
    }
    if (hasError) throw firstError
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
  // CAUTION 内联派发的错误隔离（2026-H3 round9 F1，与 digestEffectSession 同一
  //  语义的兄弟实现点）：首个订阅者抛错不得跳过其余订阅者——被跳过的订阅者
  //  既不执行也不标脏（status 停留 CLEAN），读到的是静默的陈旧值，且 Object.is
  //  判等门会拦截同值重写（无法靠"再写一次"救回，只有写入不同值或 force
  //  recompute 能追平）。digest 修复时的这段理由逐字适用于内联循环，却只落在了
  //  digest（"修复者沿被攻击的轴泛化"的又一实例）。现在四个多订阅者派发循环
  //  （triggerEffects / trigger 的去重循环 / triggerPrimitiveAtomValue 的 overflow
  //  循环 / Computed.recursiveMarkDirty）统一：全部订阅者执行完后把第一个错误
  //  抛给写入方，其余错误 console.error 上报。README §2 已成文。
  /** @internal 内联派发循环的共享错误收集器（见上方 CAUTION） */
  static reportSuppressedInlineError(err: unknown) {
    console.error('[data0] suppressed additional subscriber error in inline dispatch (the first error is propagating to the writer):', err)
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

    // CAUTION 这里不再为 CompactDep 设快路径（2026-H3 round3 mutation 审计确认为
    //  不可达死分支）：CompactDep 只存在于 primitive atom 的函数对象上（不进
    //  targetMap），其派发一律走 triggerPrimitiveAtomValue 的特化路径；本方法只
    //  接到 track() 创建的 Set 型 dep。即使未来有 CompactDep 流入，下方 spread
    //  快照对任何可迭代 dep 都正确（CompactDep 实现了 Symbol.iterator），只是
    //  少一个单订阅者微优化。
    // CAUTION 快照稳定化：effect 执行过程中可能向 dep 增删订阅，
    //  不能直接在 live Set 上迭代（新增的订阅会被本轮误触发）。
    const effects = [...(dep as unknown as Iterable<ReactiveEffect>)]
    let hasError = false
    let firstError: unknown
    for (const effect of effects) {
      if (effect !== activeEffect) {
        // CAUTION 特别注意这里，因为我们现在支持了 lazy recompute，所以可能在读的时候才重算。
        //  重算过程中可能会再次触发 trigger，因为像 atomComputed 这种是在重算的时候更新 atom 值的。
        try {
          this.triggerEffect(effect, source, type, inputInfo, debuggerEventExtraInfo)
        } catch (err) {
          if (hasError) {
            Notifier.reportSuppressedInlineError(err)
          } else {
            hasError = true
            firstError = err
          }
        }
      }
    }
    if (hasError) throw firstError
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
  let bodyThrew = false
  try {
    return fn()
  } catch (err) {
    bodyThrew = true
    throw err
  } finally {
    // CAUTION body 异常优先（2026-H3 round6 R6-3）：fn 抛错时 digest 仍必须执行
    //  （session 一旦创建必须消化，且其余订阅者不能被 body 异常牵连丢标脏），
    //  但 digest 重抛的订阅者 firstError 会按 JS 的 finally-throw 语义**静默替换**
    //  在途的 body 异常——调用方只看到订阅者错误，自己代码的原始异常完全丢失。
    //  body 异常在途时订阅者错误降级为 console.error 上报（与 digest 内
    //  "第二个及以后的错误" 的既有兜底同款），body 异常照常传播。
    if (bodyThrew) {
      try {
        notifier.digestEffectSession()
      } catch (digestErr) {
        console.error('[data0] suppressed subscriber error in batch digest (the batch body itself threw; propagating the body error):', digestErr)
      }
    } else {
      notifier.digestEffectSession()
    }
    if (__DEV__) {
      assert(ReactiveEffect.activeScopes.length === scopesDepthBefore, 'activeScopes depth not restored after batch (scope leak)')
      assert(notifier.trackStack.length === trackStackDepthBefore, 'trackStack depth not restored after batch (pause/reset imbalance)')
    }
  }
}
