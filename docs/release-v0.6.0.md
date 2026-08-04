# v0.6.0 release verification

This record separates artifact verification from performance evidence. The `v0.6.0` images and
chart were built from annotated tag commit `5157d2766b4abcb1fd257d4464c0742909733013` on
2026-08-05. Passing the checks below proves packaging, portability, and no-cost runtime behavior;
it does not establish real-model throughput or latency.

## Immutable artifacts

| Artifact | Immutable reference |
| --- | --- |
| Controller | `ghcr.io/alphasxd/agentstorm-controller@sha256:b37ae7edcbc5da61f5f1680f3aca5d5d6311c6a2b7745ebea088dde3279428ca` |
| Worker | `ghcr.io/alphasxd/agentstorm-worker@sha256:a06b8416f11355ddcfa470aa7932d9ce02546d8959a7ebee4c2ca2a53938cc41` |
| Result API | `ghcr.io/alphasxd/agentstorm-result-api@sha256:8dec868c6e4c118fdf5d0afb7645400422244c532464e5bd10d038887ebb70dd` |
| Helm chart | `oci://ghcr.io/alphasxd/charts/agentstorm:0.6.0` (`sha256:ad20aa4da8f389f21f01ebcfe807090318f51cc4fbb833403ff2e4d2f867b509`) |

For each image, the version tag and full `sha-5157d2766b4abcb1fd257d4464c0742909733013`
tag resolve to the same image-index digest. Each index contains `linux/amd64` and `linux/arm64`;
anonymous manifest access succeeds; no `latest` tag was published. All three images have an SPDX
SBOM, provenance, and a GitHub artifact attestation that passes `gh attestation verify`.

## Verification runs

- Image and chart publication: [GitHub Actions run 30929650406](https://github.com/Alphasxd/AgentStorm/actions/runs/30929650406).
- amd64 published-artifact verification: [GitHub Actions run 30930555336](https://github.com/Alphasxd/AgentStorm/actions/runs/30930555336).
- The amd64 run installed, upgraded, and uninstalled the OCI chart and passed namespace, durable
  Result API, telemetry/reliability, and KEDA scale-to-zero smokes using only published digests.
- A local arm64 kind cluster independently passed the namespace, 32-case fake SRE durable result,
  and KEDA queue smokes using the same three published digests and no local image build or load.

The macOS nested-kind run used the repository's explicit
`DISABLE_LOCAL_NETWORK_POLICIES=true` compatibility path because the local DNAT from service port
443 to the API server port is not preserved by that environment's NetworkPolicy implementation.
The public policies were unchanged and remained enabled in the amd64 GitHub gate.

## Pending canonical evidence

The fixed real-model matrix is not included in this record. It requires a user-supplied
`OPENAI_API_KEY`, an isolated OpenAI project with a USD 10 platform hard limit, and the runner's USD
8 recorded-cost stop. Until all 384 capacity executions and the separate 32-case reliability run
finish, AgentStorm does not publish throughput, tail-latency, cost, speedup, or efficiency
conclusions and M6 remains in progress.

See [the SRE benchmark protocol](../benchmarks/sre/README.md) for the immutable workload and
publishability rules.
