import shutil
import subprocess
import sys
import shlex
from pathlib import Path

import pytest


def test_register_python_argcomplete_suggests_subcommands(tmp_path: Path) -> None:
    pytest.importorskip("argcomplete")
    if shutil.which("register-python-argcomplete") is None:
        pytest.skip("register-python-argcomplete not installed")

    script = tmp_path / "barrow-cli"
    script.write_text(f'#!/usr/bin/env bash\n"{sys.executable}" -m barrow.cli "$@"\n')
    script.chmod(0o755)

    script_path = shlex.quote(str(script))
    cmd = f"""
eval "$(register-python-argcomplete {script_path})"
COMP_LINE="{script_path} "
COMP_POINT=${{#COMP_LINE}}
COMP_WORDS=({script_path} "")
COMP_CWORD=1
COMP_TYPE=9
_python_argcomplete {script_path}
printf '%s\n' "${{COMPREPLY[@]}}"
"""
    result = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    suggestions = set(result.stdout.split())
    assert {"filter", "select"} <= suggestions
