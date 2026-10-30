# anynng

[![CI](https://github.com/axiomantic/anynng/actions/workflows/ci.yml/badge.svg)](https://github.com/axiomantic/anynng/actions/workflows/ci.yml)

[![Docs](https://img.shields.io/badge/docs-latest-blue.svg)](https://axiomantic.github.io/anynng/)

[![Release](https://img.shields.io/github/v/release/axiomantic/anynng)](https://github.com/axiomantic/anynng/releases/latest)
[![License](https://img.shields.io/github/license/axiomantic/anynng)](LICENSE)
[![Nim](https://img.shields.io/badge/Nim-2.2%2B-yellow.svg)](https://nim-lang.org)

Pure-Nim multi-target (C, JS, WASM) and multi-transport (WS, TCP, IPC, UDP, Inproc) NNG / Scalability Protocols implementation

## Quick Start

```nim
import anynng

echo hello()
```

## Development

The `justfile` is the canonical command surface for common tasks:

```bash
just setup        # Install dependencies
just test         # Run test suite
just lint         # Check formatting (nph) and nimble check
just fmt          # Auto-format codebase with nph

just docs         # Extract API docs and launch local doc server
just docs-build   # Build static production documentation

just build        # Build release artifacts
```

## Upstream Updates

This project was initialized via [Polyform](https://github.com/axiomantic/polyform). To update boilerplate, CI workflows, and tooling from upstream:

```bash
uvx copier update --trust
```
