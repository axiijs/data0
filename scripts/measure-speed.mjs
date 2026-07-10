// 速度对比：node scripts/measure-speed.mjs <dist path>
// 覆盖最热路径：atom 读写、带订阅者的 trigger、effect 创建/销毁、RxList splice。
// 每个用例同时报告期间的 minor GC 次数（分配压力的直接信号）。
import path from "node:path";
import { pathToFileURL } from "node:url";
import { PerformanceObserver, constants } from "node:perf_hooks";

const distPath = process.argv[2] || "./dist/data0.js";
const data0 = await import(pathToFileURL(path.resolve(distPath)).href);
const { atom, computed, Computed, RxList, RxMap, ReactiveEffect, batch } = data0;

class LightEffect extends ReactiveEffect {
  constructor(update) {
    super();
    this.update = update;
    this.active = true;
  }
  callGetter() {
    return this.update(this);
  }
}

// gc entries 只派发给 observer，回调是异步的：每次读数前 flush 一轮宏任务
let minorGcCount = 0;
const gcObserver = new PerformanceObserver((list) => {
  for (const entry of list.getEntries()) {
    if (entry.detail?.kind === constants.NODE_PERFORMANCE_GC_MINOR) minorGcCount++;
  }
});
gcObserver.observe({ entryTypes: ["gc"] });
const flushGcEntries = () => new Promise((resolve) => setImmediate(resolve));

async function bench(name, iterations, fn) {
  // warmup
  fn(Math.min(iterations, 10000));
  await flushGcEntries();
  const gcBefore = minorGcCount;
  const start = process.hrtime.bigint();
  fn(iterations);
  const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
  await flushGcEntries();
  const gcDuring = minorGcCount - gcBefore;
  console.log(
    `${name.padEnd(46)} ${elapsed.toFixed(1).padStart(8)}ms (${iterations} iter, ${gcDuring} minor GC)`
  );
}

await bench("atom write+read (1 subscriber)", 2_000_000, (n) => {
  const a = atom(0);
  let sink = 0;
  const e = new LightEffect(() => { sink += a() });
  e.run();
  for (let i = 0; i < n; i++) a(i);
  e.destroy();
  return sink;
});

await bench("atom read untracked", 5_000_000, (n) => {
  const a = atom(1);
  let sink = 0;
  for (let i = 0; i < n; i++) sink += a.raw;
  return sink;
});

await bench("effect create+track+destroy", 200_000, (n) => {
  const a = atom(0);
  for (let i = 0; i < n; i++) {
    const e = new LightEffect(() => a());
    e.run();
    e.destroy();
  }
});

await bench("computed chain depth 50, single write", 20_000, (n) => {
  const source = atom(0);
  let last = source;
  for (let d = 0; d < 50; d++) {
    const prev = last;
    last = computed(() => prev() + 1);
  }
  for (let i = 0; i < n; i++) source(i);
});

await bench("fanout 1 atom -> 100 light effects", 20_000, (n) => {
  const a = atom(0);
  let sink = 0;
  const effects = Array.from({ length: 100 }, () => {
    const e = new LightEffect(() => { sink += a() });
    e.run();
    return e;
  });
  for (let i = 0; i < n; i++) a(i);
  effects.forEach((e) => e.destroy());
  return sink;
});

await bench("computed create+run+destroy", 200_000, (n) => {
  const a = atom(0);
  for (let i = 0; i < n; i++) {
    const internal = new Computed(() => a() + 1);
    internal.run([], true);
    internal.destroy();
  }
});

await bench("RxList splice churn (1000 rows x 200)", 200, (n) => {
  const list = new RxList([]);
  const rows = Array.from({ length: 1000 }, (_, i) => ({ id: i }));
  for (let i = 0; i < n; i++) {
    list.splice(0, list.data.length, ...rows);
    list.splice(0, list.data.length);
  }
  list.destroy();
});

await bench("forEach(1000) in computed + splice middle", 2_000, (n) => {
  const list = new RxList(Array.from({ length: 1000 }, (_, i) => i));
  const sum = computed(() => {
    let s = 0;
    list.forEach((v) => { s += v });
    return s;
  });
  for (let i = 0; i < n; i++) {
    list.splice(500, 1, i);
  }
  sum();
  list.destroy();
});

await bench("RxList(1000).map create+destroy", 500, (n) => {
  const rows = Array.from({ length: 1000 }, (_, i) => ({ id: i }));
  for (let i = 0; i < n; i++) {
    const list = new RxList(rows.slice());
    const mapped = list.map((item) => ({ id: item.id + 1 }));
    mapped.destroy();
    list.destroy();
  }
});

await bench("RxMap create+destroy", 50_000, (n) => {
  for (let i = 0; i < n; i++) {
    const map = new RxMap({ a: 1, b: 2 });
    map.destroy();
  }
});

await bench("batch update 100 atoms x rounds", 20_000, (n) => {
  const atoms = Array.from({ length: 100 }, () => atom(0));
  const effects = atoms.map((a) => {
    const e = new LightEffect(() => a());
    e.run();
    return e;
  });
  for (let i = 0; i < n; i++) {
    batch(() => {
      for (let k = 0; k < 100; k++) atoms[k](i + k);
    });
  }
  effects.forEach((e) => e.destroy());
});

gcObserver.disconnect();
