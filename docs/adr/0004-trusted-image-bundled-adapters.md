# ADR 0004: Trusted image-bundled Adapter plugins

Status: Accepted

`target.adapterEntrypoint` selects a Python factory already installed in the immutable Worker image.
The Controller treats it as validated configuration and never imports plugin code. Inline scripts,
remote downloads, and ConfigMap code are rejected because they undermine reproducibility and expand
the secret/sandbox boundary. Plugins are trusted dependencies and receive only provider, model, and
base URL through their factory context. They still share the Worker process and may read its
environment, so this interface does not isolate a plugin from Provider credentials.
