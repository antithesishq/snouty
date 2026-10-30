import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


STUB = '''#!/usr/bin/env python3
import os
from pathlib import Path
import sys
root = Path(os.environ["SUPERVISOR_TEST_ROOT"])
args = sys.argv[1:]
with (root / "calls").open("a") as calls:
    calls.write(Path(sys.argv[0]).name + " " + " ".join(args) + "\\n")
if Path(sys.argv[0]).name == "systemctl" and args[0] == "show":
    if "--property=ControlGroup" in args:
        print("/test")
    elif "--property=Result" in args:
        print("exit-code" if (root / "failure").exists() else "success")
    else:
        print("active" if (root / "active").exists() else "inactive")
'''


def run_case(mode: str) -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "stub"
        stub.write_text(STUB)
        stub.chmod(0o755)
        for name in ("systemctl", "podman", "fault_injector_update", "journalctl"):
            (bin_dir / name).symlink_to(stub)
        env = dict(os.environ, SUPERVISOR_TEST_ROOT=str(root), PATH=f"{bin_dir}:{os.environ['PATH']}")
        assets = Path(__file__).resolve().parents[3] / "src/simulate/assets"

        if mode == "readiness":
            watch_file = root / "watch"
            watch_file.touch()
            libc = ctypes.CDLL(None)
            fd = libc.inotify_init1(0)
            assert fd >= 0
            (root / "cgroup.procs").write_text(f"{os.getpid()}\n")
            source = (assets / "start-composer.sh").read_text().replace(
                '"/sys/fs/cgroup$composer_cgroup/cgroup.procs"', str(root / "cgroup.procs")
            )
            process = subprocess.Popen(["bash", "-c", source], env=env)
            try:
                time.sleep(0.3)
                assert process.poll() is None, "returned before the watch was ready"
                assert libc.inotify_add_watch(fd, os.fsencode(watch_file), 0x08) >= 0
                assert process.wait(timeout=5) == 0
            finally:
                process.terminate()
                process.wait(timeout=5)
                os.close(fd)
            calls = (root / "calls").read_text()
            assert "systemctl start antithesis-test-composer" in calls
        elif mode == "status":
            source = (assets / "status.sh").read_text()
            (root / "active").touch()
            result = subprocess.run(["bash", "-c", source], env=env, capture_output=True, text=True)
            assert result.returncode == 0 and json.loads(result.stdout) == "running", result
            assert "--pause" not in (root / "calls").read_text()
            (root / "active").unlink()
            result = subprocess.run(["bash", "-c", source], env=env, capture_output=True, text=True)
            assert result.returncode == 0 and json.loads(result.stdout) == "completed", result
            assert "fault_injector_update --pause" in (root / "calls").read_text()
            (root / "calls").unlink()
            (root / "failure").touch()
            result = subprocess.run(["bash", "-c", source], env=env, capture_output=True, text=True)
            assert result.returncode != 0
            calls = (root / "calls").read_text()
            assert calls.index("fault_injector_update --pause") < calls.index("journalctl")
        elif mode == "recreate":
            containers = root / "containers"
            crun = root / "crun"
            containers.mkdir()
            crun.mkdir()
            source = (assets / "recreate.sh").read_text().replace(
                "/opt/antithesis/containers", str(containers)
            ).replace("/run/crun", str(crun))
            result = subprocess.run(["bash", "-c", source], env=env, capture_output=True, text=True)
            assert result.returncode == 0 and json.loads(result.stdout) == "idle", result
            assert not (root / "calls").exists()
            for name in ("live", "stale"):
                container = containers / name
                container.mkdir()
                (container / "commands.json").write_text('{"commands":["suite/serial_driver_test"]}')
            (crun / "live").mkdir()
            (crun / "live/config.json").touch()
            result = subprocess.run(["bash", "-c", source], env=env, capture_output=True, text=True)
            assert result.returncode == 0 and json.loads(result.stdout) == "restarted", result
            calls = (root / "calls").read_text().splitlines()
            assert "down --volumes --remove-orphans" in calls[0], calls
            assert "up -d --no-build --pull=never" in calls[1], calls
            assert (containers / "live").exists()
            assert not (containers / "stale").exists()
        else:
            raise AssertionError(mode)


if __name__ == "__main__":
    run_case(sys.argv[1])
