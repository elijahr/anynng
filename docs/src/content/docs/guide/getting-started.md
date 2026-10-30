---
title: Quick Start
description: Getting started with anynng
---

## Installation

Add `anynng` to your `.nimble` file or install via Nimble:

```bash
nimble install anynng
```

## Basic Usage

Import the package in your Nim code:

```nim title="src/main.nim"
import anynng

echo hello()
```

## Architecture Flowchart

```mermaid
graph LR
    User[Developer / Caller] --> API[anynng API]
    API --> Core[Internal Logic]
    Core --> Result[Return Value]
```

## Running Tests & Building

Use `just` as the canonical command runner:

```bash
just test    # Run full test suite
just lint    # Run pre-commit & formatting checks
just fmt     # Auto-format with nph
just docs    # Launch local documentation server
```
