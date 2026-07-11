# Contributing to data0

提交代码前请先阅读 [AGENTS.md](./AGENTS.md)，其中的测试、性能和 review 证据要求适用于人工贡献者与 AI agent。

## Review 证据要求

- 语义承诺面以 [README.md](./README.md) 为准；立案前先对照 AGENTS.md 的「架构决策与已知语义边界」一节，其中列出的行为是既定架构语义，不作为缺陷报告或修复。
- 缺陷修复必须先有能在未修复代码上失败的确定性复现。
- **修复必须向上游还债**（AGENTS.md 3.1）：除实例回归外，说明缺陷的等价类，并落一层能覆盖该类的常驻防线（构造性原语 / dev 断言 / 差分测试 / 交错枚举）。
- PR 应写明触发条件、预期结果、实际结果和执行命令。
- 无法安全运行的供应链或发布问题，应使用临时目录和假命令隔离复现。
- 静态事实与尚未验证的风险必须明确标注，不能作为“已复现缺陷”陈述；“无问题”结论必须注明验证方法及其可达范围。
- 已知失败可以暂存为 `test.fails`；修复该问题的提交必须将其转换为普通回归测试。

最低验证基线（CI 在每个 push/PR 上强制执行）：

```bash
pnpm test --run
pnpm type-check
pnpm build
```

热路径改动附 `pnpm bench` 分支对比；周期性测试质量审计用 `pnpm exec stryker run --mutate 'src/<module>.ts'`（见 AGENTS.md「Mutation 审计」）。
