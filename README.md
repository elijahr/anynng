# anynng

[![Status: Experimental](https://img.shields.io/badge/status-experimental-orange.svg)](#status)
[![CI](https://github.com/elijahr/anynng/actions/workflows/ci.yml/badge.svg)](https://github.com/elijahr/anynng/actions/workflows/ci.yml)
[![Docs](https://img.shields.io/badge/docs-latest-blue.svg)](https://elijahr.github.io/anynng/)
[![Release](https://img.shields.io/github/v/release/elijahr/anynng)](https://github.com/elijahr/anynng/releases/latest)
[![License](https://img.shields.io/github/license/elijahr/anynng)](LICENSE)
[![Nim](https://img.shields.io/badge/Nim-2.2%2B-yellow.svg)](https://nim-lang.org)

> [!WARNING]
> **Experimental / Pre-Alpha Software — Not Ready for Production**
> This project is an active research and development prototype. APIs, ABI layouts, and wire formats are unstable and subject to breaking changes without notice. Do not use in production systems.

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
