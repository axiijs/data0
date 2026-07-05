// 逐对象“称重”：node --expose-gc scripts/measure-retained.mjs [dist path]
// 测量 atom / LightBinding 风格 effect / computed / RxList 的 GC 后保留字节数
import path from "node:path";
import { pathToFileURL } from "node:url";

const distPath = process.argv[2] || "./dist/data0.js";
const data0 = await import(pathToFileURL(path.resolve(distPath)).href);
const { atom, computed, RxList, ReactiveEffect } = data0;

const N = 20000;

function gcNow() {
  global.gc();
  global.gc();
}

function used() {
  gcNow();
  return process.memoryUsage().heapUsed;
}

function weigh(name, create) {
  // 预热一次，排除首次隐藏类/IC 分配
  let warm = new Array(100);
  for (let i = 0; i < 100; i++) warm[i] = create(i);
  warm = null;
  const keep = new Array(N);
  const before = used();
  for (let i = 0; i < N; i++) keep[i] = create(i);
  const after = used();
  const perItem = (after - before) / N;
  console.log(`${name.padEnd(44)} ${perItem.toFixed(1)}B/个`);
  return keep; // 防止提前回收
}

class LightEffect extends ReactiveEffect {
  constructor(update) {
    super();
    if (update) this.update = update;
    this.active = true;
  }
  callGetter() {
    return this.update(this);
  }
}

const results = [];
results.push(weigh("primitive atom(string)", (i) => atom(`label ${i}`)));
results.push(weigh("primitive atom + 1 subscriber(track)", (i) => {
  const a = atom(`label ${i}`);
  const e = new LightEffect(() => a());
  e.run();
  return [a, e];
}));
results.push(weigh("LightEffect alone (no deps)", () => new LightEffect(() => {})));
results.push(weigh("computed(atom dep)", (i) => {
  const src = atom(i);
  return [src, computed(() => src() + 1)];
}));
results.push(weigh("bare computed(no deps)", () => computed(() => 1)));
results.push(weigh("RxList(empty)", () => new RxList([])));
console.log("done", results.length);
