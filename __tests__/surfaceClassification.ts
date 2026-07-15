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
    RxList: ['constructor', 'doSplice', 'dispatchStructuralThen', 'ensureAtomIndex', 'addAtomIndexesDep', 'removeAtomIndexesDep', 'pruneIndexKeyDeps', 'onUntrack', 'destroyResources', 'raw', 'indexKeyDeps'],
    RxMap: ['constructor', 'destroyResources'],
    RxSet: ['constructor', 'destroyResources'],
}

/**
 * 下游反馈标记（2026-H3 round4）：消费者契约回放（方法 13/16）只能钉住下游
 * **真实消费**的表面；标记为 'none' 的面没有任何生产反馈，缺陷只能靠仓内
 * 覆盖发现，review 立项与覆盖投入按更高权重对待（R4-1 教训：map(index) ×
 * reorder 在下游零使用，双重搬移潜伏两年）。
 *
 * grep 依据（2026-07-15，axii/axle src 全扫）：两个下游都只把 RxList/RxMap/
 * RxSet 当**原始容器**使用（构造 + 变更方法 + RxListHost 消费 triggerInfo 协议），
 * 不调用任何派生算子（map/filter/toSorted/slice/concat/groupBy/indexBy/toMap/
 * toSet/reduce/find 与 selection 家族、RxSet 代数、RxMap.keys/values/entries/size）。
 * 下游用法变化时更新本表（新增使用 ⇒ 升级标记并补消费者契约钉扎）。
 */
export const DOWNSTREAM_FEEDBACK: Record<string, 'axii' | 'axle' | 'both' | 'none'> = {
    'RxList.map': 'none',
    'RxList.filter': 'none',
    'RxList.toSorted': 'none',
    'RxList.slice': 'none',
    'RxList.concat': 'none',
    'RxList.groupBy': 'none',
    'RxList.toSet': 'none',
    'RxList.findIndex': 'none',
    'RxList.find': 'none',
    'RxList.some': 'none',
    'RxList.every': 'none',
    'RxList.indexBy': 'none',
    'RxList.toMap': 'none',
    'RxList.reduce': 'none',
    'RxList.reduceToAtom': 'none',
    'RxList.length': 'none',
    'RxList.createSelection': 'none',
    'RxList.createSelections': 'none',
    'RxList.createIndexKeySelection': 'none',
    'RxMap.keys': 'none',
    'RxMap.values': 'none',
    'RxMap.entries': 'none',
    'RxMap.size': 'none',
    'RxSet.difference': 'none',
    'RxSet.intersection': 'none',
    'RxSet.symmetricDifference': 'none',
    'RxSet.union': 'none',
    'RxSet.toList': 'none',
    'RxSet.has': 'none',
    'RxSet.size': 'none',
    'RxSet.isSubsetOf': 'none',
    'RxSet.isSupersetOf': 'none',
    'RxSet.isDisjointFrom': 'none',
}
