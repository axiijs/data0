import { Notifier } from './notify'
import {ReactiveEffect} from "./reactiveEffect.js";
import {trackRetainedDepEffectAdded, trackRetainedDepEffectRemoved} from "./retainedDiagnostics";

export type Dep = DepCollection & TrackedMarkers & DepHostBookkeeping

/**
 * wasTracked and newTracked maintain the status for several levels of effect
 * tracking recursion. One bit per level is used to define whether the dependency
 * was/is tracked.
 */
type TrackedMarkers = {
  /**
   * wasTracked
   */
  w: number
  /**
   * newTracked
   */
  n: number
}

/**
 * 宿主记账（仅 notifier.targetMap 的 keyed dep 使用，由 track() 在创建时写入）：
 * 退订到空时把 dep 从宿主 keyToDepMap 摘除。没有它，"订阅不同 key → 退订"的
 * 循环（虚拟滚动的 at(index)、分页的 RxMap.get(key)）会在长活 target 上留下
 * 无界增长的空 Dep 条目——每个曾被订阅过的 key 一个，直到 target 销毁才释放。
 * primitive atom 的 CompactDep 不需要（每 atom 恒一个，随 atom 回收）。
 */
type DepHostBookkeeping = {
  host?: Map<any, Dep>
  hostKey?: any
}

type DepCollection = Iterable<ReactiveEffect> & {
  add(effect: ReactiveEffect): DepCollection
  delete(effect: ReactiveEffect): boolean
  has(effect: ReactiveEffect): boolean
}

export const createDep = (effects?: ReactiveEffect[]): Dep => {
  const dep = new Set<ReactiveEffect>(effects) as unknown as Dep
  dep.w = 0
  dep.n = 0
  return dep
}

/**
 * Most primitive atoms only have one subscriber in Axii's light binding path.
 * Keep that common case out of a native Set's backing storage, while preserving
 * the small Set-like surface used by the notifier.
 *
 * single/overflow are exposed (internal) so the notifier's trigger fast path can
 * dispatch to a lone subscriber without creating a generator + array snapshot.
 */
export class CompactDep implements Dep {
  w = 0
  n = 0
  /** @internal 0/1 个订阅者时的存储；升级到 >=2 时迁入 overflow */
  single?: ReactiveEffect
  /** @internal */
  overflow?: Set<ReactiveEffect>

  constructor(effects?: ReactiveEffect[]) {
    effects?.forEach(effect => this.add(effect))
  }

  add(effect: ReactiveEffect): this {
    if (this.overflow) {
      this.overflow.add(effect)
      return this
    }

    if (!this.single) {
      this.single = effect
    } else if (this.single !== effect) {
      this.overflow = new Set([this.single, effect])
      this.single = undefined
    }

    return this
  }

  delete(effect: ReactiveEffect): boolean {
    if (this.overflow) {
      const deleted = this.overflow.delete(effect)
      if (deleted && this.overflow.size === 1) {
        const [remaining] = this.overflow
        this.single = remaining
        this.overflow = undefined
      }
      return deleted
    }

    if (this.single !== effect) return false
    this.single = undefined
    return true
  }

  has(effect: ReactiveEffect): boolean {
    return this.overflow ? this.overflow.has(effect) : this.single === effect
  }

  *[Symbol.iterator](): IterableIterator<ReactiveEffect> {
    if (this.overflow) {
      yield* this.overflow
    } else if (this.single) {
      yield this.single
    }
  }
}

export const createCompactDep = (effects?: ReactiveEffect[]): Dep => new CompactDep(effects)

// dep 是否已无任何订阅者（订阅者退订后 dep 对象本身还留在各处的记账 Map 里）
export const isDepEmpty = (dep: Dep): boolean => {
    if (dep instanceof CompactDep) {
        return dep.single === undefined && (dep.overflow === undefined || dep.overflow.size === 0)
    }
    return (dep as unknown as Set<ReactiveEffect>).size === 0
}

/**
 * 退订使 dep 变空时把它从宿主 keyToDepMap 摘除（见 DepHostBookkeeping）。
 * 只在移除路径（cleanup/finalizeDepMarkers/restoreEffectDeps 的 delete 分支）调用，
 * track 热路径零开销。身份检查防御"同 key 已被新 dep 占位"的误删。
 */
export const pruneEmptyDepFromHost = (dep: Dep) => {
    const host = dep.host
    if (host !== undefined && host.get(dep.hostKey) === dep && isDepEmpty(dep)) {
        host.delete(dep.hostKey)
    }
}

/**
 * 重订阅一个可能已被摘除宿主的 dep 时挂回宿主。两个既有路径会"先退订清空、
 * 再重添加"同一个 dep 实例：async effect 收尾（completeTracking 先 cleanup 再重放
 * asyncTracks）与失败重算的依赖恢复（restoreEffectDeps）。若期间同 key 已有新 dep
 * 占位（错误恢复路径上用户 getter 可能已重新 track 过同一 key），把 effect 并入
 * 现行 dep（触发路径只认宿主里的那一个）。返回实际生效的 dep；调用方在返回值
 * 不同于入参时需把它也登记进 effect.deps。
 */
export const reattachDepToHost = (dep: Dep, effect: ReactiveEffect): Dep => {
    const host = dep.host
    if (host === undefined) return dep
    const current = host.get(dep.hostKey)
    if (current === dep) return dep
    if (current === undefined) {
        host.set(dep.hostKey, dep)
        return dep
    }
    current.add(effect)
    return current
}

export const wasTracked = (dep: Dep): boolean => (dep.w & Notifier.trackOpBit) > 0

export const newTracked = (dep: Dep): boolean => (dep.n & Notifier.trackOpBit) > 0

export const initDepMarkers = ({ deps }: ReactiveEffect) => {
  if (deps.length) {
    for (let i = 0; i < deps.length; i++) {
      deps[i].w |= Notifier.trackOpBit // set was tracked
    }
  }
}

export const finalizeDepMarkers = (effect: ReactiveEffect) => {
  const { deps } = effect
  if (deps.length) {
    let ptr = 0
    for (let i = 0; i < deps.length; i++) {
      const dep = deps[i]
      if (wasTracked(dep) && !newTracked(dep)) {
        if (dep.delete(effect)) {
          trackRetainedDepEffectRemoved(dep)
          pruneEmptyDepFromHost(dep)
        }
      } else {
        deps[ptr++] = dep
      }
      // clear bits
      dep.w &= ~Notifier.trackOpBit
      dep.n &= ~Notifier.trackOpBit
    }
    deps.length = ptr
  }
}

/**
 * Restore the exact dependency set that existed before a failed recompute.
 *
 * A getter may throw after prepareTracking has either marked or removed the old
 * deps and after it has partially collected new deps. Error recovery must retain
 * the last successful dependency graph: otherwise a throw before the first read
 * permanently unsubscribes the effect and no later source write can retry it.
 *
 * This runs only on the error path, so the temporary Set has no hot-path cost.
 */
export const restoreEffectDeps = (effect: ReactiveEffect, previousDeps?: Dep[]) => {
  const previous = previousDeps ?? []
  const previousSet = new Set(previous)

  for (const dep of effect.deps) {
    if (!previousSet.has(dep) && dep.delete(effect)) {
      trackRetainedDepEffectRemoved(dep)
      pruneEmptyDepFromHost(dep)
    }
  }

  for (const dep of previous) {
    if (!dep.has(effect)) {
      dep.add(effect)
      trackRetainedDepEffectAdded(dep)
    }
    // 恢复的 dep 可能在失败重算的 cleanup 中被（瞬时清空而）摘除宿主：挂回；
    // 用户 getter 若在抛错前已对同 key 建立了新 dep，则并入现行 dep。
    const effective = reattachDepToHost(dep, effect)
    if (effective !== dep && !previousSet.has(effective)) {
      previousSet.add(effective)
      previous.push(effective)
    }
  }

  effect.deps = previous
}
