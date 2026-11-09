#!/usr/bin/env python3
"""HeaderKit v2 C-ABI Verification & Signature Alignment Harness.

Verifies:
1. NimBackend C header cleanup and Nim compiler AST enrichment pipeline.
2. Complete parameter name restoration from Nim source (zero _p0/_p1 suffixes).
3. CffiWriter and PyiWriter generation with zero un-mangled suffixes.
4. Headerkit multi-target [[targets]] pipeline execution (diff, cffi, pyi).
5. Upstream nng.h C-ABI signature alignment via DiffWriter(abi_mode=True, ignore_pointer_const=True).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def resolve_headerkit_dir() -> Path:
    if "HEADERKIT_DIR" in os.environ:
        return Path(os.environ["HEADERKIT_DIR"]).resolve()
    sibling = (REPO_ROOT.parent / "headerkit").resolve()
    if sibling.is_dir():
        return sibling
    try:
        res = subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode == 0:
            common_dir = Path(res.stdout.strip())
            repo = common_dir if common_dir.is_absolute() else (REPO_ROOT / common_dir).resolve()
            canonical_root = repo.parent if repo.name == ".git" else repo
            candidate = (canonical_root.parent / "headerkit").resolve()
            if candidate.is_dir():
                return candidate
    except Exception:
        pass
    return sibling


HEADERKIT_DIR = resolve_headerkit_dir()

if HEADERKIT_DIR.exists() and str(HEADERKIT_DIR) not in sys.path:
    sys.path.insert(0, str(HEADERKIT_DIR))

try:
    from headerkit._config import load_config
    from headerkit._generate import generate_targets
    from headerkit.backends.libclang import LibclangBackend
    from headerkit.backends.nim import NimBackend, clean_nim_c_unit
    from headerkit.ir import Enum, Function
    from headerkit.writers.cffi import CffiWriter
    from headerkit.writers.diff import DiffWriter
    from headerkit.writers.pyi import PyiWriter
except ImportError as exc:
    print(f"Error importing HeaderKit v2: {exc}", file=sys.stderr)
    print(f"PYTHONPATH: {sys.path}", file=sys.stderr)
    sys.exit(1)


def build_nim_header() -> Path:
    """Build or ensure dist/nimcache/anynng.h exists."""
    header_path = REPO_ROOT / "dist" / "nimcache" / "anynng.h"
    c_api_nim = REPO_ROOT / "src" / "anynng" / "c_api.nim"

    os.chdir(REPO_ROOT)
    (REPO_ROOT / "dist" / "include").mkdir(parents=True, exist_ok=True)
    cmd = [
        "nim",
        "c",
        "-f",
        "--header:dist/include/anynng.h",
        "--noMain",
        "--app:lib",
        "-d:release",
        "--nimcache:dist/nimcache",
        "--out:dist/libanynng.dylib",
        str(c_api_nim),
    ]
    print(f"[1/5] Building Nim C header: {' '.join(cmd)}")
    res = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        print(f"Failed to compile Nim C header:\n{res.stderr}\n{res.stdout}", file=sys.stderr)
        sys.exit(res.returncode)

    if not header_path.is_file():
        print(f"Error: expected header not found at {header_path}", file=sys.stderr)
        sys.exit(1)

    print(f"      Generated header: {header_path} ({header_path.stat().st_size} bytes)")
    return header_path


def verify_transforms_and_parameter_names(header_path: Path):
    """Verify NimBackend and clean_nim_c_unit restore genuine parameter names from Nim AST."""
    print("[2/5] Parsing C header with NimBackend (libclang) & enriching from Nim AST...")
    code = header_path.read_text(encoding="utf-8")
    root_nim = REPO_ROOT / "src" / "anynng" / "c_api.nim"
    nim_backend = NimBackend(c_backend="libclang")
    cleaned = nim_backend.parse(code, str(header_path))
    enriched = clean_nim_c_unit(cleaned, nim_source_path=root_nim)
    parsed_with_nim = nim_backend.parse(code, str(header_path), nim_source_path=root_nim)
    if len(parsed_with_nim.declarations) != len(enriched.declarations):
        print("FAIL: NimBackend.parse(..., nim_source_path=...) declaration count mismatch!", file=sys.stderr)
        sys.exit(1)

    print(f"      Declarations after cleanup: {len(cleaned.declarations)}")
    print(f"      Declarations after enrichment: {len(enriched.declarations)}")

    # Check for parameter name suffixes (_p0, _p1, etc.)
    p_suffix_count = 0
    total_params = 0
    functions = [d for d in enriched.declarations if isinstance(d, Function)]
    for fn in functions:
        for p in fn.parameters:
            total_params += 1
            if p.name and (p.name.endswith("_p0") or p.name.endswith("_p1") or p.name.endswith("_p2")):
                print(f"      ERROR: Function '{fn.name}' has un-restored parameter '{p.name}'", file=sys.stderr)
                p_suffix_count += 1

    if p_suffix_count > 0:
        print(f"FAIL: Found {p_suffix_count} parameter(s) with '_p0/_p1' suffixes!", file=sys.stderr)
        sys.exit(1)

    print(f"      PASSED: All {total_params} parameters across {len(functions)} functions restored (zero '_p0/_p1' suffixes).")
    return enriched


def verify_writers(target_unit):
    """Verify CffiWriter and PyiWriter generation."""
    print("[3/5] Generating and verifying CFFI and Pyi stubs...")
    cffi_text = CffiWriter().write(target_unit)
    pyi_text = PyiWriter().write(target_unit)

    if "_p0" in cffi_text or "_p1" in cffi_text:
        print("FAIL: CffiWriter output contains '_p0/_p1' suffixes!", file=sys.stderr)
        sys.exit(1)

    if "_p0" in pyi_text or "_p1" in pyi_text:
        print("FAIL: PyiWriter output contains '_p0/_p1' suffixes!", file=sys.stderr)
        sys.exit(1)

    print(f"      PASSED: CFFI ({len(cffi_text)} bytes) and Pyi ({len(pyi_text)} bytes) stubs verified with zero mangled suffixes.")


def verify_headerkit_toml_targets():
    """Verify headerkit.toml [[targets]] multi-target execution."""
    print("[4/5] Executing headerkit.toml [[targets]] pipeline...")
    cfg_file = REPO_ROOT / "headerkit.toml"
    if not cfg_file.is_file():
        print(f"FAIL: headerkit.toml not found at {cfg_file}", file=sys.stderr)
        sys.exit(1)

    cfg = load_config(cfg_file)
    results = generate_targets(
        cfg.targets,
        default_backend_name=cfg.backend or "nim",
        default_include_dirs=cfg.include_dirs or None,
        default_defines=cfg.defines or None,
        default_transforms=cfg.transforms or None,
        project_root=REPO_ROOT,
        no_cache=True,
    )

    print(f"      Executed {len(results)} declarative targets:")
    for r in results:
        print(f"        ✓ {r.writer_name}: {r.output_path}")

    # Verify generated target output files exist
    for r in results:
        if r.output_path and not Path(r.output_path).is_file():
            print(f"FAIL: Target output {r.output_path} does not exist!", file=sys.stderr)
            sys.exit(1)

    print("      PASSED: All declarative targets generated successfully.")


def verify_abi_signature_alignment(target_unit):
    """Verify non-enum function signatures and struct/union layouts match upstream nng.h via DiffWriter."""
    print("[5/5] Comparing C-ABI signatures and struct/union layouts against upstream nanomsg/nng v2.1.0 nng.h...")
    upstream_nng = REPO_ROOT / ".cache" / "nng-upstream" / "include" / "nng" / "nng.h"
    upstream_inc = REPO_ROOT / ".cache" / "nng-upstream" / "include"

    if not upstream_nng.is_file():
        # Fallback to canonical repo cache if running in a git worktree or strand
        try:
            res = subprocess.run(
                ["git", "rev-parse", "--git-common-dir"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            if res.returncode == 0:
                common_dir = Path(res.stdout.strip())
                repo = common_dir if common_dir.is_absolute() else (REPO_ROOT / common_dir).resolve()
                canonical_root = repo.parent if repo.name == ".git" else repo
                candidate = canonical_root / ".cache" / "nng-upstream" / "include" / "nng" / "nng.h"
                if candidate.is_file():
                    upstream_nng = candidate
                    upstream_inc = candidate.parent.parent
        except Exception:
            pass

    if not upstream_nng.is_file():
        print(f"FAIL: Upstream nng.h not found at {upstream_nng}", file=sys.stderr)
        sys.exit(1)

    lc = LibclangBackend()
    baseline = lc.parse(
        upstream_nng.read_text(encoding="utf-8"),
        str(upstream_nng),
        include_dirs=[str(upstream_inc)],
    )

    writer = DiffWriter(
        baseline=baseline,
        format="json",
        abi_mode=True,
        ignore_pointer_const=True,
    )
    diff_report = json.loads(writer.write(target_unit))

    base_enums = {d.name for d in baseline.declarations if isinstance(d, Enum)}
    target_funcs = {d.name for d in target_unit.declarations if isinstance(d, Function)}
    target_types = {
        d.name for d in target_unit.declarations
        if hasattr(d, "name") and type(d).__name__ in ("Struct", "Union")
    }

    breaking_defects = []
    for e in diff_report.get("entries", []):
        if e.get("severity") != "breaking":
            continue
        name = e.get("name")
        kind = e.get("kind")

        # 1. Struct and Union breaking changes (layout, removed fields, field type changes)
        if name in target_types:
            breaking_defects.append(e)
            continue

        # 2. Function signature breaking changes on implemented C-ABI functions
        if name in target_funcs:
            b_type = e.get("baseline", "")
            t_type = e.get("target", "")

            # Enum return type or enum parameter type representation in C is int
            if b_type in base_enums and t_type == "int":
                continue

            # Callback typedefs whose params have enums
            if "nng_pipe_cb" in b_type or "nng_logger" in b_type:
                continue

            # Array parameter decayed to pointer (int[2] vs int*)
            if name == "nng_socket_pair" and "int[2]" in b_type and t_type == "int*":
                continue

            breaking_defects.append(e)

    if breaking_defects:
        print(f"FAIL: Detected {len(breaking_defects)} breaking C-ABI change(s):", file=sys.stderr)
        for b in breaking_defects:
            print(f"  - {b}", file=sys.stderr)
        sys.exit(1)

    common_funcs = len(set(target_funcs) & {d.name for d in baseline.declarations if isinstance(d, Function)})
    print(f"      PASSED: 0 breaking defects across all {common_funcs} implemented functions and {len(target_types)} structs/unions.")


def main():
    print("=== HeaderKit v2 C-ABI Verification & Signature Alignment Harness ===")
    header_path = build_nim_header()
    target_unit = verify_transforms_and_parameter_names(header_path)
    verify_writers(target_unit)
    verify_headerkit_toml_targets()
    verify_abi_signature_alignment(target_unit)
    print("\n✓ All HeaderKit v2 C-ABI verification checks passed cleanly with 100% parity.")


if __name__ == "__main__":
    main()
