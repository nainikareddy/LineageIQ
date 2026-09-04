from __future__ import annotations

import shutil
from pathlib import Path

from synthetic.generate import generate, generated_hashes


def test_generator_matches_manifest_and_is_byte_identical(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"

    first_result = generate(first, progress=False)
    second_result = generate(second, progress=False)

    assert first_result["model_counts"] == {
        "staging": 10,
        "intermediate": 10,
        "marts": 10,
    }
    assert all(first_result["checks"].values())
    assert first_result == second_result
    assert generated_hashes(first) == generated_hashes(second)

    shutil.rmtree(second)
    generate(first, progress=False)
    assert all(first_result["checks"].values())
