/**
 * 集合表面成员分类(单一事实源,非 spec)。
 *
 * 从 coverageInventory.spec.ts 抽出(2026-H3 round3):入口语义账本
 * (entryPointSemanticsInventory)与覆盖账本共享同一份成员清单——两本账本
 * 各自强制"原型成员必须被分类",清单一处修改两处生效,防止双账漂移。
 */
export const READS: Record<string, string[]> = {
    RxList: ['at', 'forEach', 'toArray'],
    RxMap: ['get', 'forEach'],
    RxSet: ['forEach', 'toArray'],
}

export const MUTATIONS: Record<string, string[]> = {
    RxList: ['push', 'pop', 'shift', 'unshift', 'splice', 'spliceArray', 'set', 'clear', 'reorder', 'reposition', 'swap', 'sortSelf', 'replaceData'],
    RxMap: ['set', 'delete', 'clear', 'replace', 'replaceData'],
    RxSet: ['add', 'delete', 'clear', 'replace', 'replaceData'],
}

export const INTERNAL: Record<string, string[]> = {
    RxList: ['constructor', 'doSplice', 'ensureAtomIndex', 'addAtomIndexesDep', 'removeAtomIndexesDep', 'pruneIndexKeyDeps', 'onUntrack', 'destroyResources', 'raw', 'indexKeyDeps'],
    RxMap: ['constructor', 'destroyResources'],
    RxSet: ['constructor', 'destroyResources'],
}
