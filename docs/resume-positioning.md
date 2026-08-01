# Resume positioning

Do not add AgentStorm to a resume until at least M1 is complete and publicly reproducible.

Evidence worth collecting:

- Number of parallel Jobs, worker concurrency, and completed Agent cases.
- P50/P95/P99 latency, success rate, error taxonomy, and token usage.
- Controller recovery and cancellation tests.
- Baseline-versus-candidate regression examples.
- One fault-injection experiment with a clear conclusion.
- Public release, documentation, CI status, issues, and external users or contributors.

Suggested wording after M1:

> 独立设计并实现 Kubernetes 原生 AI Agent 评测与压测平台，基于 controller-runtime 构建
> AgentTestRun 自定义资源与控制器，通过 Indexed Jobs 执行分布式 Agent 任务，支持并发控制、
> 数据分片、生命周期管理及质量阈值校验，完成可复现的本地集群部署与自动化测试。

Additional wording after M2/M3 is actually complete:

> 建设幂等结果采集与基线对比能力，并基于 OpenTelemetry、Prometheus 构建模型调用、工具调用
> 及评测结果的可观测链路。

Replace generic claims with measured figures only after running reproducible benchmarks. Do not claim
KEDA, ClickHouse, fault injection, or centralized observability until the corresponding milestone is
actually implemented.
