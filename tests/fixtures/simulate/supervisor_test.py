import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


STUB = '''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

root = Path(os.environ["SUPERVISOR_TEST_ROOT"])
name = Path(sys.argv[0]).name
args = sys.argv[1:]
if name == "systemctl":
    if args[0] == "start":
        (root / "active").touch()
        with (root / "events").open("a") as events:
            events.write("composer-start\\n")
    elif args[0] == "is-active":
        sys.exit(0 if (root / "active").exists() else 3)
    elif args[0] == "show":
        print("/test" if "--property=ControlGroup" in args else "success")
elif name == "podman":
    with (root / "compose-calls").open("a") as calls:
        calls.write(" ".join(args) + "\\n")
    if "up" in args:
        (root / "state" / "setup-ready").touch()
elif name == "systemd-run":
    (root / "timer-args").write_text(json.dumps(args))
    with (root / "events").open("a") as events:
        events.write("timer-start\\n")
elif name == "touch":
    for arg in args:
        Path(arg).touch()
        if arg == str(root / "new"):
            with (root / "composer-notified").open("a") as notified:
                notified.write("touch\\n")
            with (root / "events").open("a") as events:
                events.write("touch\\n")
            if (root / "mode").read_text() == "composer" and (root / "composer-notified").read_text().count("touch") == 1:
                (root / "active").unlink()
'''

ENTROPY = '''#!/usr/bin/env python3
import os
from pathlib import Path
import sys

root = Path(os.environ["SUPERVISOR_TEST_ROOT"])
with (root / "entropy-calls").open("a") as calls:
    calls.write(" ".join(sys.argv[1:]) + "\\n")
with (root / "events").open("a") as events:
    events.write("entropy-" + sys.argv[2] + "\\n")
with (root / "new").open("a"):
    pass
'''


def run_case(mode: str) -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        (root / "mode").write_text(mode)
        state = root / "state"
        state.mkdir()
        (state / "setup-ready").touch()
        (state / "cgroup.procs").write_text(f"{os.getpid()}\n")
        watch_file = root / "new"
        watch_file.touch()
        libc = ctypes.CDLL(None)
        fd = libc.inotify_init1(0)
        assert fd >= 0
        assert libc.inotify_add_watch(fd, os.fsencode(watch_file), 0x08) >= 0

        bin_dir = root / "bin"
        bin_dir.mkdir()
        stub = bin_dir / "stub"
        stub.write_text(STUB)
        stub.chmod(0o755)
        for name in ("systemctl", "podman", "systemd-run", "touch"):
            (bin_dir / name).symlink_to(stub)
        entropy = root / "fake-add_entropy"
        entropy.write_text(ENTROPY)
        entropy.chmod(0o755)

        if mode == "composer":
            commands = root / "containers" / "container"
            commands.mkdir(parents=True)
            (commands / "commands.json").write_text(
                '{"commands":["suite/serial_driver_test"]}'
            )
            crun = root / "crun" / "container"
            crun.mkdir(parents=True)
            (crun / "config.json").touch()

        start = Path(__file__).resolve().parents[3] / "src/simulate/assets/start.sh"
        wrapper = start.read_text().split("<<'ENTROPY'\n", 1)[1].split(
            "\nENTROPY", 1
        )[0]
        wrapper = wrapper.replace(
            "#!/run/current-system/sw/bin/bash", "#!/usr/bin/env bash"
        ).replace(
            "export PATH=/run/current-system/sw/bin:/run/current-system/sw/sbin",
            f"export PATH={bin_dir}:{os.environ['PATH']}",
        ).replace("/nix/store/*-add_entropy", str(entropy))
        wrapper_path = state / "add-entropy"
        wrapper_path.write_text(wrapper)
        wrapper_path.chmod(0o755)

        source = start.read_text().split("<<'SUPERVISOR'\n", 1)[1].split(
            "\nSUPERVISOR", 1
        )[0]
        source = source.replace(
            "export PATH=/run/current-system/sw/bin:/run/current-system/sw/sbin",
            f"export PATH={bin_dir}:{os.environ['PATH']}",
        )
        source = source.replace(
            "state_dir=/run/antithesis-local-injection", f"state_dir={state}"
        )
        source = source.replace(
            '"/sys/fs/cgroup$composer_cgroup/cgroup.procs"',
            '"$state_dir/cgroup.procs"',
        )
        source = source.replace("/opt/antithesis/containers", str(root / "containers"))
        source = source.replace("/run/crun", str(root / "crun"))
        source = source.replace("/opt/antithesis/rollouts/new", str(root / "new"))
        script = root / "supervisor.sh"
        script.write_text(source)
        env = dict(os.environ, SUPERVISOR_TEST_ROOT=str(root))
        process = subprocess.Popen(
            [
                "bash",
                str(script),
                "/config/docker-compose.yaml",
                "once" if mode == "once" else "restart",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                calls_file = root / "entropy-calls"
                notified = root / "composer-notified"
                ready = (
                    calls_file.exists()
                    and (root / "timer-args").exists()
                    and notified.exists()
                )
                if mode == "composer":
                    ready = ready and len(notified.read_text().splitlines()) >= 2
                if ready:
                    break
                if process.poll() is not None:
                    raise AssertionError(process.communicate()[1].decode())
                time.sleep(0.05)
            else:
                process.terminate()
                _, stderr = process.communicate(timeout=5)
                raise AssertionError(
                    f"supervisor did not start timer or composer ({mode}): {stderr.decode()}"
                )

            timer_args = json.loads((root / "timer-args").read_text())
            assert "--on-active=30s" in timer_args
            assert "--on-unit-active=30s" in timer_args
            assert timer_args[-2:] == [str(wrapper_path), "false"]
            subprocess.run(["bash", str(wrapper_path), "false"], env=env, check=True)

            calls = (root / "entropy-calls").read_text().splitlines()
            assert len(calls) == 2, calls
            seeds_and_force = [call.split() for call in calls]
            assert all(seed.isdecimal() for seed, _ in seeds_and_force), calls
            assert [force for _, force in seeds_and_force] == ["true", "false"], calls
            events = (root / "events").read_text().splitlines()
            assert events[:3] == ["entropy-true", "composer-start", "touch"], events
            assert events.count("timer-start") == 1, events
            if mode == "composer":
                assert events.count("composer-start") == 2, events
                assert events.count("touch") == 2, events
                compose_calls = (root / "compose-calls").read_text()
                assert "down --volumes --remove-orphans" in compose_calls
                assert "up -d --no-build --pull=never" in compose_calls
            else:
                assert not (root / "compose-calls").exists()
                assert events.count("touch") == 1, events
                if mode == "once":
                    assert process.wait(timeout=5) == 0
                else:
                    assert process.poll() is None
        finally:
            process.terminate()
            process.communicate(timeout=5)
            os.close(fd)


if __name__ == "__main__":
    run_case(sys.argv[1])
