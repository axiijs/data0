# Contributing to data0

提交代码前请先阅读 [AGENTS.md](./AGENTS.md)，其中的测试、性能和 review 证据要求适用于人工贡献者与 AI agent。

## Review 证据要求

- 缺陷修复必须先有能在未修复代码上失败的确定性复现。
- PR 应写明触发条件、预期结果、实际结果和执行命令。
- 无法安全运行的供应链或发布问题，应使用临时目录和假命令隔离复现。
- 静态事实与尚未验证的风险必须明确标注，不能作为“已复现缺陷”陈述。
- 已知失败可以暂存为 `test.fails`；修复该问题的提交必须将其转换为普通回归测试。

最低验证基线：

```bash
pnpm test --run
pnpm type-check
pnpm build
```
