// using literal strings instead of numbers so that it's easier to inspect
// debugger events

// CAUTION 不用 `const enum`：发布的 d.ts 里 ambient const enum 会让开启
//  isolatedModules（现代 Vite/esbuild/SWC 工程的默认配置）的下游无法引用成员
//  （axle 曾被迫用字符串字面量 + as cast 绕开）。常量对象 + 字面量联合类型
//  在值/类型两个位置都保持原用法（TrackOpTypes.GET / type: TrackOpTypes）不变。
export const TrackOpTypes = {
  ATOM: 'atom',
  GET: 'get',
  HAS: 'has',
  ITERATE: 'iterate',
  METHOD: 'method',
  EXPLICIT_KEY_CHANGE: 'explicit_key_change'
} as const
export type TrackOpTypes = typeof TrackOpTypes[keyof typeof TrackOpTypes]

export const TriggerOpTypes = {
  ATOM: 'atom',
  SET: 'set',
  ADD: 'add',
  DELETE: 'delete',
  CLEAR: 'clear',
  METHOD: 'method',
  EXPLICIT_KEY_CHANGE: 'explicit_key_change'
} as const
export type TriggerOpTypes = typeof TriggerOpTypes[keyof typeof TriggerOpTypes]
