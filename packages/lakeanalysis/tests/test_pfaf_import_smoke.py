"""Pfafstetter module smoke / import tests (P2).

Validates that psycopg (not psycopg2) is used and that fundamental
imports resolve without crash.
"""

from __future__ import annotations

import sys
import subprocess
from pathlib import Path


def test_pfaf_lookup_import_resolves() -> None:
    import lakeanalysis.artificial.pfaf.lookup  # noqa: F401


def test_pfaf_store_import_resolves() -> None:
    import lakeanalysis.artificial.pfaf.store  # noqa: F401


def test_pfaf_nearest_import_resolves() -> None:
    import lakeanalysis.artificial.pfaf.nearest  # noqa: F401


def test_no_psycopg2_import_in_pfaf(tmp_path: Path) -> None:
    """Verify pfaf modules use 'import psycopg' not 'import psycopg2'."""
    src_dir = Path(__file__).resolve().parents[3] / "packages" / "lakeanalysis" / "src" / "lakeanalysis"
    pfaf_dir = src_dir / "artificial" / "pfaf"

    result = subprocess.run(
        ["rg", "--no-heading", r"\bpsycopg2\b", str(pfaf_dir)],
        capture_output=True, text=True,
    )
    assert result.stdout == "", (
        f"psycopg2 reference found in pfaf modules:\n{result.stdout}"
    )


def test_no_psycopg2_import_in_cli_sync(tmp_path: Path) -> None:
    """Verify cli/sync.py uses 'import psycopg' not 'import psycopg2'."""
    src_dir = Path(__file__).resolve().parents[3] / "packages" / "lakeanalysis" / "src" / "lakeanalysis"
    sync_file = src_dir / "cli" / "sync.py"

    result = subprocess.run(
        ["rg", "--no-heading", r"\bpsycopg2\b", str(sync_file)],
        capture_output=True, text=True,
    )
    assert result.stdout == "", (
        f"psycopg2 reference found in cli/sync.py:\n{result.stdout}"
    )


def test_pfaf_uses_psycopg_import() -> None:
    import ast
    import lakeanalysis.artificial.pfaf.lookup
    import lakeanalysis.artificial.pfaf.store

    def _check_imports(module) -> None:
        source = Path(module.__file__).read_text()
        tree = ast.parse(source)
        imports = [
            node.names[0].name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom)
            for name in node.names
        ]
        assert "psycopg" in imports, f"{module.__name__} should import psycopg"

    _check_imports(lakeanalysis.artificial.pfaf.lookup)
    _check_imports(lakeanalysis.artificial.pfaf.store)
