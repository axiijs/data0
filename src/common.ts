import { Atom, isAtom } from "./atom.js";
import {copyTriggerInfoPayload, DirtyCallback, Computed, GetterContext} from "./computed.js";
import { TriggerInfo } from "./notify.js";
import { TrackOpTypes, TriggerOpTypes } from "./operations.js";
import { RxList } from "./RxList.js";
import { RxMap } from "./RxMap.js";
import { RxSet } from "./RxSet.js";

// CAUTION  autorun/once 是用来执行用户代码的。
//  一定自己不能有 digest session 或者在其他的 digest session 中，
//  因为用户很可能在 autorun 中进行reactive操作然后立刻读 computed，并且期望 computed 是立刻执行的，保持数据一致性。
//  所以创建的时候通过 preventEffectSession 来控制自己，通过 schedule 默认为 nextJob 来控制不在其他中。
const nextJob = (run:(...args:[]) => any) => Promise.resolve().then(() => {
    // CAUTION 调度上下文没有可传播异常的调用方：不捕获的话，用户 fn 在 rerun 时抛错
    //  会变成 unhandled rejection（Node 默认直接崩溃进程）。computed 已复位为 DIRTY，
    //  下次触发可以重试。
    try {
        run()
    } catch (err) {
        console.error('[data0] uncaught error in scheduled rerun:', err)
    }
})
/**
 * @category Miscellaneous
 */
export function autorun(fn: (context: GetterContext) => any, scheduleRerun: DirtyCallback|true = nextJob) {
    const instance = new Computed(fn, undefined, scheduleRerun, undefined, undefined, true)
    instance.run([], true)
    // const instance = new Autorun(fn)
    return () => {
        instance.destroy()
    }
}
/**
 * @category Miscellaneous
 */
export function once(fn:() => any, scheduleRerun: DirtyCallback|true = nextJob) {
    let stopFn: () => any
    let stopped = false
    let instance:Computed|undefined = new Computed(() => {
        // 双重保险。防止在 nextJob 真正 destroy 之前又触发了。
        if(stopped) return
        const shouldStop = fn()
        if (shouldStop) {
            stopped = true
            // CAUTION 一定不能立刻 stop，因为立刻 stop 会删除自己身上的 deps 等数据.
            //  不能正确完成 completeTracking。这会使得 dep 上的 n/w 计数无法正确清清除。
            nextJob(() => stopFn!())
        }
    }, undefined, scheduleRerun, undefined, undefined, true)
    // }, undefined, true, undefined, undefined, undefined, true)

    stopFn = () => {
        instance?.destroy()
        instance = undefined
    }
    // const instance = new Autorun(fn)
    instance.run([], true)
    // 也支持外部手动 stop
    return stopFn
}
/**
 * @category Miscellaneous
 */
export function oncePromise(fn:() => any, scheduleRerun: DirtyCallback|true = nextJob) {
    return new Promise((resolve, reject) => {
        once(() => {
            // fn 抛异常时 reject 并停止监听，否则 promise 永远不会 settle
            let result
            try {
                result = fn()
            } catch (err) {
                reject(err)
                return true
            }
            if (result) {
                resolve(result)
            }
            return result
        }, scheduleRerun)
    })
}

/**
 * @category Miscellaneous
 */
export function onChange(source:Atom|RxList<any>|RxSet<any>|RxMap<any, any>, handler: (...args:any[]) => any) {
    const instance = new Computed(function(this: Computed){
        if (isAtom(source)) {
            this.manualTrack(source, TrackOpTypes.ATOM, 'value')
        } else if (source instanceof RxList) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
        } else if (source instanceof RxSet) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
        } else if (source instanceof RxMap) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
        }
        
    }, function applyPatch(this:Computed, data:any, triggerInfos:TriggerInfo[]) {
        // 防御副本(载荷所有权契约,README「参数契约」):info 的 argv/methodResult
        // 是对全部订阅者共享的广播,handler 是任意用户代码——给它独立副本
        // (外层数组 + 一层嵌套:RxSet.replace 的 [newItems,deletedItems]、
        // RxMap 的 entry 对、reorder 的 Order 对),改写不再毒化兄弟订阅者。
        // onChange 是观察 API 而非热路径,O(载荷) 拷贝可接受;曾用 dev 冻结
        // 执法,但冻结落在 trigger 热路径上(ABBA: push +20%/swap +52%),弃用。
        handler(triggerInfos.map(copyTriggerInfoPayload))
    })

    instance.run([], true)
    
    return function destroy() {
        instance.destroy()
    }
}
