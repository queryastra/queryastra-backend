import subprocess
import sys
import textwrap
import threading
from pathlib import Path
from uuid import uuid4

from app.core import instance_executors

_BACKEND_ROOT = Path(__file__).resolve().parents[3]


async def test_stuck_threads_lists_only_threads_still_running():
    release = threading.Event()
    instance_id = uuid4()
    executor = instance_executors._get_or_create_executor(instance_id)
    executor.submit(release.wait)

    instance_executors.shutdown_all()
    assert len(instance_executors.stuck_threads()) == 1

    release.set()
    for thread in instance_executors._released_threads:
        thread.join(timeout=5)
    assert instance_executors.stuck_threads() == []


def test_process_exits_even_with_a_thread_stuck_in_a_driver_call():
    script = textwrap.dedent(
        """
        import asyncio, threading
        from uuid import uuid4
        from app.core import instance_executors

        async def main():
            hung = threading.Event()
            asyncio.get_running_loop().run_in_executor(
                instance_executors._get_or_create_executor(uuid4()), hung.wait
            )
            await asyncio.sleep(0.2)
            instance_executors.shutdown_all()
            instance_executors.exit_if_threads_stuck(grace_seconds=0.5)

        asyncio.run(main())
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=_BACKEND_ROOT, timeout=30, capture_output=True
    )
    assert result.returncode == 0, result.stderr.decode(errors="replace")
