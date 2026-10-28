"""Metadata imports must not initialize the entire pipeline."""

import os
import subprocess
import sys
from pathlib import Path


def test_package_metadata_does_not_load_agents_or_validate_credentials():
    source = Path(__file__).parents[1] / "src"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import maia, sys; "
            "assert maia.__version__; "
            "assert 'atlas.config' not in sys.modules; "
            "assert 'prefect' not in sys.modules",
        ],
        env={**os.environ, "PYTHONPATH": str(source)},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
