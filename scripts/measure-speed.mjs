// 速度对比：node scripts/measure-speed.mjs <dist path>
// 覆盖最热路径：atom 读写、带订阅者的 trigger、effect 创建/销毁、RxList splice
import path from "node:path";
import { pathToFileURL } from "node:url";

const distPath = process.argv[2] || "./dist/data0.js";
const data0 = await import(pathToFileURL(path.resolve(distPath)).href);
const { atom, RxList, ReactiveEffect, batch } = data0;

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

function bench(name, iterations, fn) {
  // warmup
  fn(Math.min(iterations, 10000));
  const start = process.hrtime.bigint();
  fn(iterations);
  const elapsed = Number(process.hrtime.bigint() - start) / 1e6;
  console.log(`${name.padEnd(40)} ${elapsed.toFixed(1)}ms (${iterations} iter)`);
}

bench("atom write+read (1 subscriber)", 2_000_000, (n) => {
  const a = atom(0);
  let sink = 0;
  const e = new LightEffect(() => { sink += a() });
  e.run();
  for (let i = 0; i < n; i++) a(i);
  e.destroy();
  return sink;
});

bench("atom read untracked", 5_000_000, (n) => {
  const a = atom(1);
  let sink = 0;
  for (let i = 0; i < n; i++) sink += a.raw;
  return sink;
});

bench("effect create+track+destroy", 200_000, (n) => {
  const a = atom(0);
  for (let i = 0; i < n; i++) {
    const e = new LightEffect(() => a());
    e.run();
    e.destroy();
  }
});

bench("RxList splice churn (1000 rows x 200)", 200, (n) => {
  const list = new RxList([]);
  const rows = Array.from({ length: 1000 }, (_, i) => ({ id: i }));
  for (let i = 0; i < n; i++) {
    list.splice(0, list.data.length, ...rows);
    list.splice(0, list.data.length);
  }
  list.destroy();
});

bench("batch update 100 atoms x rounds", 20_000, (n) => {
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
