# v0.6 demo

`agentstorm.tape` is the reproducible recording source for the public demo. Record it only after the
OCI chart and three image digests are published, replacing any retained source-only output with the
released references.

The demo shows:

1. Helm installation from GHCR.
2. A no-cost fake run of the 32-case SRE diagnostic Agent.
3. The complete Agent model/tool call summary.
4. KEDA Worker resources returning to zero.

The recording cluster must already have KEDA 2.20, an external Result API/storage stack, the
`agentstorm-result-auth` Secret in the execution namespace, and a local Result API port-forward on
`127.0.0.1:18080`. If that retained stack includes a development Controller, scale it to zero while
the Helm-installed Controller owns the demo Run, then restore it after recording. With
[VHS](https://github.com/charmbracelet/vhs) installed:

```bash
vhs demo/agentstorm.tape
```

The generated GIF is a release asset, not a source-of-truth benchmark. The committed fake evidence
proves execution shape and lifecycle behavior only; it intentionally contains no performance data.
