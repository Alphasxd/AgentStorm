# Contributing

## Development checks

```bash
make fmt
make vet
make test
```

Changes to the CRD must update all of the following in one pull request:

- `api/v1alpha1` Go types
- generated/deep-copy behavior
- CRD OpenAPI schema
- sample manifests
- controller and worker configuration contract
- relevant documentation and tests

Keep provider integrations behind the worker adapter interface. The controller must not import model
SDKs or depend on prompt/tool schemas.

Never commit API keys, real prompts, proprietary datasets, raw production traces, or generated result
files containing sensitive content.
