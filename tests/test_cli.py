from __future__ import annotations

import json
from pathlib import Path

from lineageiq.cli import main

ROOT = Path(__file__).resolve().parents[1]


def test_audit_and_eval_run_on_generated_stack(tmp_path: Path, capsys: object) -> None:
    audit_file = tmp_path / "audit.json"
    assert (
        main(
            [
                "audit",
                "--repo-root",
                str(ROOT),
                "--semantic-backend",
                "reference",
                "--output",
                str(audit_file),
            ]
        )
        == 0
    )
    audit = json.loads(audit_file.read_text())
    assert audit["judge"]["backend"] == "reference"
    assert len(audit["findings"]) == 7

    assert (
        main(
            [
                "eval",
                "--repo-root",
                str(ROOT),
                "--audit-file",
                str(audit_file),
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert '"recall": 1.0' in output
    assert '"false_positive_count_against_negatives": 0' in output
