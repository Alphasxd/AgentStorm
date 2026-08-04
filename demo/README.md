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
`127.0.0.1:18080`. With [VHS](https://github.com/charmbracelet/vhs) installed:

```bash
vhs demo/agentstorm.tape
```

The generated GIF is a release asset, not a source-of-truth benchmark. Canonical performance data
comes only from the checksummed files under the published benchmark evidence directory.
