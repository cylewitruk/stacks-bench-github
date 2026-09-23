#!/usr/bin/env python3
"""Exercise restore-check schema detection against a mocked current database."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/pg-restore-check.sh"


class RestoreCheckTest(unittest.TestCase):
    def test_current_task_submission_schema_is_validated(self) -> None:
        with tempfile.TemporaryDirectory() as raw_temp:
            temp = Path(raw_temp)
            backup = temp / "backup.tar.zst"
            backup.write_bytes(b"mock")
            docker = temp / "docker"
            docker.write_text(
                textwrap.dedent(
                    """\
                    #!/usr/bin/env bash
                    set -euo pipefail
                    if [[ "${1:-}" == inspect ]]; then
                        echo true
                        exit 0
                    fi
                    [[ "${1:-}" == exec ]] || exit 1
                    shift
                    [[ "${1:-}" == -i ]] && shift
                    shift
                    command=${1:-}
                    shift || true
                    case "$command" in
                        sh) cat >/dev/null ;;
                        pg_restore|createdb|dropdb|rm) ;;
                        psql)
                            sql=${*: -1}
                            if [[ "$sql" == *"to_regclass('public.task_submission')"* ]]; then
                                echo t
                            elif [[ "$sql" == *"to_regclass('public.benchmark_group')"* ]]; then
                                echo f
                            elif [[ "$sql" == *"unlinked_jobs="* ]]; then
                                cat <<'EOF'
                    unlinked_jobs=0
                    jobs=4
                    submissions=3
                    specs=4
                    build_steps=4
                    run_steps=3
                    runnable_specs=3
                    empty_submissions=0
                    orphan_specs=0
                    orphan_jobs=0
                    cross_submission_jobs=0
                    orphan_steps=0
                    cross_submission_steps=0
                    EOF
                            else
                                exit 1
                            fi
                            ;;
                        *) exit 1 ;;
                    esac
                    """
                ),
                encoding="utf-8",
            )
            docker.chmod(0o755)
            zstd = temp / "zstd"
            zstd.write_text(
                "#!/usr/bin/env bash\nprintf 'mock archive'\n",
                encoding="utf-8",
            )
            zstd.chmod(0o755)

            env = os.environ.copy()
            env["PATH"] = f"{temp}:{env['PATH']}"
            result = subprocess.run(
                [SCRIPT, "--no-migrate", backup],
                check=True,
                capture_output=True,
                text=True,
                env=env,
            )

        self.assertIn("current task-submission schema", result.stdout)
        self.assertIn("submission -> spec -> run checklist", result.stdout)
        self.assertIn("PASS — backup restores", result.stdout)
        self.assertNotIn("pre-v14 backup", result.stdout)


if __name__ == "__main__":
    unittest.main()
