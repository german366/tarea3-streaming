"""Ejecuta el notebook en modo headless y vuelca los resultados a `evidence/`.

Uso: `uv run python scripts/make_evidence.py` (o `make evidence`).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"


def main() -> int:
    EVIDENCE.mkdir(exist_ok=True)
    sys.path.insert(0, str(ROOT))

    import notebook  # noqa: E402  (importa el módulo Marimo)

    _outputs, defs = notebook.app.run()

    dumps = {
        "oracle_totals.json": defs["oracle_totals"],
        "beam_batch_totals.json": defs["beam_totals"],
        "test_stream_panes.json": defs["stream_panes"],
        "sink_upsert_materialized.json": defs["upsert_rows"],
        "sink_upsert_audit.json": defs["upsert_audit"],
        "sink_append_materialized.json": defs["append_rows"],
    }
    for name, payload in dumps.items():
        (EVIDENCE / name).write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n",
            encoding="utf-8",
        )
        print(f"escrito evidence/{name} ({len(payload)} filas)")

    result = subprocess.run(
        ["uv", "run", "pytest", "-v", "-p", "no:cacheprovider", "-W", "ignore"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    (EVIDENCE / "pytest.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
    print(f"escrito evidence/pytest.txt (exit={result.returncode})")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
