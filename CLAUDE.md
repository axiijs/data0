# CLAUDE.md

本仓库的 agent 工作约定以 [AGENTS.md](./AGENTS.md) 为单一事实源。

进行代码 review 时必须遵守其中的“真实复现优先”原则，严格区分动态复现、静态确认和待验证假设。

立案前先对照 AGENTS.md 的「架构决策与已知语义边界」一节：菱形依赖 glitch、batch 内读 computed 得旧值、构造/replace 采纳外部容器引用是既定架构语义，不作为缺陷处理。
