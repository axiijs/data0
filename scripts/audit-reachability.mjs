// GC 可达性审计（方法 18 常驻资产，周期性运行，不进 CI）：
//   pnpm build && node --expose-gc scripts/audit-reachability.mjs [dist path]
//
// retainedDiagnostics 是计数器（只见登记过的 effect），measure-retained 是称重
// （只见增量字节）；两者都不能证明 destroy 后对象图真的对 GC 不可达。本脚本用
// WeakRef 在真实 GC 下断言：
//   1. destroy/stop 后的派生结构必须可回收（source 长活）；
//   2. 未 destroy 的派生必须不可回收（阴性对照，防审计本身失真）；
//   3. depsMap 记账残留有界（与 deepReview2026H3Round2.spec.ts 的缺陷类 11
//      回归互为补充：spec 钉行为，本脚本钉真实堆）。
// 分配必须发生在独立函数栈帧内：同帧局部变量会被保守栈扫描 pin 住，产生假阳性。
import path from "node:path";
import { pathToFileURL } from "node:url";

if (typeof global.gc !== "function") {
  console.error("run with --expose-gc");
  process.exit(2);
}

const distPath = process.argv[2] || "./dist/data0.js";
const data0 = await import(pathToFileURL(path.resolve(distPath)).href);
const { atom, computed, destroyComputed, RxList, RxMap, autorun, Notifier } = data0;

async function gcSettle() {
  for (let i = 0; i < 5; i++) {
    global.gc();
    await new Promise((r) => setTimeout(r, 5));
  }
}

let failures = 0;
function report(name, ok, detail = "") {
  if (!ok) failures++;
  console.log(`${ok ? "OK  " : "FAIL"} ${name}${detail ? " — " + detail : ""}`);
}

const longLivedSource = new RxList([1, 2, 3, 4, 5]);
const keepAtom = atom(1);

// ---- 1. destroy 后可回收（每个分配都在独立栈帧内完成并 destroy）----
function allocMapDerived() {
  const mapped = longLivedSource.map((x) => x * 2);
  mapped.destroy();
  return new WeakRef(mapped);
}
function allocFilterDerived() {
  const filtered = longLivedSource.filter((x) => x % 2 === 0);
  filtered.destroy();
  return new WeakRef(filtered);
}
function allocSortedDerived() {
  const sorted = longLivedSource.toSorted((a, b) => a - b);
  sorted.destroy();
  return new WeakRef(sorted);
}
function allocComputed() {
  const c = computed(() => keepAtom() + 1);
  destroyComputed(c);
  return new WeakRef(c);
}
function allocAutorun() {
  const seen = [];
  const stop = autorun(() => seen.push(keepAtom()));
  stop();
  return new WeakRef(stop);
}
function allocGroupBy() {
  const grouped = longLivedSource.groupBy((x) => x % 2);
  for (const g of grouped.data.values()) g.destroy();
  grouped.destroy();
  return new WeakRef(grouped);
}

for (const [name, alloc] of [
  ["RxList.map derived (destroyed, source alive)", allocMapDerived],
  ["RxList.filter derived (destroyed, source alive)", allocFilterDerived],
  ["RxList.toSorted derived (destroyed, source alive)", allocSortedDerived],
  ["computed on atom (destroyed, atom alive)", allocComputed],
  ["autorun stopped (atom alive)", allocAutorun],
  ["groupBy derived (destroyed)", allocGroupBy],
]) {
  const ref = alloc();
  await gcSettle();
  report(`collectable: ${name}`, ref.deref() === undefined);
}

// ---- 2. 阴性对照：存活派生必须不可回收 ----
function allocLive() {
  const mapped = longLivedSource.map((x) => x * 3);
  return [new WeakRef(mapped), mapped];
}
{
  const [ref, keep] = allocLive();
  await gcSettle();
  report("negative control: live derived stays reachable", ref.deref() !== undefined);
  keep.destroy();
}

// ---- 3. depsMap 记账残留有界（订阅不同 key → 退订 的 churn）----
const notifier = Notifier.instance;
function depsMapSize(target) {
  const m = notifier.targetMap.get(target);
  return m ? m.size : 0;
}
{
  const list = new RxList(Array.from({ length: 10000 }, (_, i) => i));
  for (let i = 0; i < 10000; i++) {
    const c = computed(() => list.at(i));
    destroyComputed(c);
  }
  const size = depsMapSize(list);
  report("ledger bounded: RxList.at() churn ×10000", size <= 8, `depsMap entries=${size}`);
  list.destroy();
}
{
  const map = new RxMap([]);
  for (let i = 0; i < 10000; i++) map.set(`k${i}`, i);
  for (let i = 0; i < 10000; i++) {
    const c = computed(() => map.get(`k${i}`));
    destroyComputed(c);
  }
  const size = depsMapSize(map);
  report("ledger bounded: RxMap.get() churn ×10000", size <= 8, `depsMap entries=${size}`);
  map.destroy();
}

console.log(failures ? `\n${failures} failure(s)` : "\nall checks passed");
process.exit(failures ? 1 : 0);
