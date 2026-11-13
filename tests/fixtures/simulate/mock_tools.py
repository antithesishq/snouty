import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

root = Path(__file__).resolve().parent.parent
args = sys.argv[1:]
name = Path(sys.argv[0]).name
mode = (root / 'mode').read_text()

def event(value):
    with (root / 'cycle_events').open('a') as output:
        output.write(value + '\n')

def record(name, value):
    (root / name).write_text(str(value))

def write_image_archive(path, tags):
    import hashlib, io, tarfile
    layer = io.BytesIO()
    with tarfile.open(fileobj=layer, mode='w') as archive:
        info = tarfile.TarInfo('app')
        info.size = 2
        archive.addfile(info, io.BytesIO(b'ok'))
    layer = layer.getvalue()
    config = json.dumps({'rootfs': {'type': 'layers', 'diff_ids': ['sha256:' + hashlib.sha256(layer).hexdigest()]}}).encode()
    manifest = json.dumps([{'Config': 'config.json', 'RepoTags': tags, 'Layers': ['layer.tar']}]).encode()
    with tarfile.open(path, 'w') as archive:
        for member, data in (('config.json', config), ('layer.tar', layer), ('manifest.json', manifest)):
            info = tarfile.TarInfo(member)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))

if name == 'docker-compose':
    if 'version' in args:
        print('2.39.0')
    elif 'config' in args:
        print(json.dumps({'services': {'workload': {'image': 'workload:test'}}}))
    else:
        sys.exit('unexpected compose command: ' + repr(args))
elif name == 'docker':
    if args == ['--version']:
        print('Docker version 29.1.3, build fixture')
    elif args[:2] == ['image', 'inspect']:
        reference = args[-1]
        platform = args[3] if args[2:3] == ['--platform'] else 'linux/amd64'
        if reference != 'workload:test' and mode == 'inspect-failure':
            sys.exit('Cannot connect to the Docker daemon fixture')
        if reference != 'workload:test' and mode in ('missing-environment', 'pull-failure', 'arm64') and not (root / 'pulled').exists():
            sys.exit('Error: No such image: ' + reference)
        if reference != 'workload:test':
            record('environment_image', reference)
        architecture = 'arm64' if platform == 'linux/arm64' else 'amd64'
        if mode == 'arm64-wrong-workload' and reference == 'workload:test':
            architecture = 'amd64'
        image_id = 'b' if architecture == 'arm64' else 'a'
        print(json.dumps([{'Id': 'sha256:' + image_id * 64, 'Architecture': architecture}]))
    elif args[0] == 'cp':
        Path(args[-1]).write_bytes(b'environment ISO fixture')
    elif args[0] == 'save':
        record('save_args', json.dumps(args))
        output = args.index('--output') + 1
        write_image_archive(Path(args[output]), args[output + 1:])
    elif args[0] == 'pull':
        record('pull_args', json.dumps(args))
        if mode == 'pull-failure':
            sys.exit('registry denied fixture')
        record('pulled', 'environment:test')
    elif args[0] in ('create', 'rm'):
        if args[0] == 'create':
            record('create_args', json.dumps(args))
        print('fixture-container')
    else:
        sys.exit('unexpected engine command: ' + repr(args))
elif name == 'ssh':
    record('ssh_invoked', 'yes')
    assert (root / 'ssh_ready').exists(), 'SSH invoked before readiness marker'
    assert args[0] == '-F', args
    config = Path(args[1])
    record('ssh_config', config.read_text())
    identity = next(line.split(maxsplit=1)[1] for line in config.read_text().splitlines() if line.strip().startswith('IdentityFile '))
    private_key = config.parent / identity
    record('key_mode', oct(private_key.stat().st_mode & 0o777))
    if args[2:] == ['-o', 'LogLevel=ERROR', '-tt', 'environment']:
        print('environment shell ready', flush=True)
        for line in sys.stdin:
            command = line.strip()
            if command == 'whoami':
                print('root', flush=True)
            elif command == 'exit':
                sys.exit(0)
            else:
                sys.exit('unexpected shell input: ' + repr(command))
        sys.exit(0)
    assert args[2] == 'environment', args
    options = args[3:-1]
    command = args[-1]
    if command == 'true':
        record('readiness_options', json.dumps(options))
        if not (root / 'ssh_ready').exists():
            sys.exit(255)
        if mode == 'slow-login':
            time.sleep(6)
        if mode == 'login-timeout':
            time.sleep(60)
        if mode == 'login-qemu-exit':
            os.kill(int((root / 'qemu_pid').read_text()), signal.SIGTERM)
            time.sleep(60)
        if mode == 'auth-failure':
            sys.exit('root@127.0.0.1: Permission denied (publickey).')
    elif command == 'bash -s':
        script = sys.stdin.read()
        if script == 'poweroff -f\n':
            record('poweroff_requested', 'yes')
            if mode in ('poweroff-hangs', 'poweroff-second-interrupt'):
                while True:
                    time.sleep(1)
            if mode == 'poweroff-fails':
                sys.exit('poweroff failed fixture')
            os.kill(int((root / 'qemu_pid').read_text()), signal.SIGUSR1)
            sys.exit(255)
        elif 'systemctl stop' in script:
            sys.exit('unexpected service cleanup')
        elif 'shmctl --thread_pause_enable' in script:
            record('thread_pause_enabled', 'yes')
        elif '/opt/antithesis/bin/fault_scripts/randomization/configure.py' in script:
            assert not (root / 'compose_started').exists()
            configure_path = '/opt/antithesis/bin/fault_scripts/randomization/configure.py'
            capture = 'capture() { if [ "$#" -gt 0 ]; then printf "%s\\0" "$@"; fi; }\n'
            result = subprocess.run(
                ['bash', '-s'],
                input=capture + script.replace(configure_path, 'capture'),
                text=True, capture_output=True, check=True,
            )
            record('configure_args', json.dumps([configure_path] + result.stdout.split('\0')[:-1]))
            if mode == 'configure-failure':
                print('configure.py configuration details')
                print('configure.py rejected configuration', file=sys.stderr)
                sys.exit(42)
        elif script == 'fault_injector_update --pause\n' or 'configure-composer' in script:
            record('faults_paused', 'yes')
            event('pause')
            if mode == 'fault-pause-failure':
                sys.exit(1)
        elif 'has_test_commands=' in script:
            print(json.dumps('autonomous' if mode in ('no-commands', 'first-only') else 'composed'))
        elif 'remove_stale_composer_registrations()' in script:
            event('pause')
            event('recreate')
            record('recreate_started', 'yes')
            if mode == 'restart-blocked':
                while True:
                    time.sleep(1)
        elif 'up -d' in script:
            record('start_script', script)
            log = Path.cwd() / 'instrumentation.log'
            contents = "11.2 [workload] [JSON] '{\"antithesis_assert\":{\"message\":\"balance stays positive\",\"assert_type\":\"always\",\"must_hit\":true,\"hit\":true,\"condition\":false}}'\n12.0 [workload] [STDOUT] 'still running after assertion'\n"
            if mode == 'fault-annotation':
                contents = "11.0 [fault_injector] [JSON] '{\"fault\":{\"name\":\"clog\",\"type\":\"network\",\"affected_nodes\":[\"workload\"],\"max_duration\":2}}'\n11.2 [workload] [JSON] '{\"antithesis_assert\":{\"message\":\"balance stays positive\",\"assert_type\":\"always\",\"must_hit\":true,\"hit\":true,\"condition\":false}}'\n"
            if mode == 'malformed-json':
                contents = "12.0 [workload] [JSON] '{broken json}'\n12.1 [workload] [STDOUT] 'still running after malformed line'\n"
            if mode in ('composer-failure', 'clean-once', 'missing-environment', 'default-image', 'arm64', 'poweroff-fails', 'poweroff-hangs', 'poweroff-second-interrupt'):
                contents = '12.0 [workload] [STDOUT] \'workload running\'\n'
            if mode.startswith('blocked-output'):
                contents = ''
                with log.open('a') as output:
                    for _ in range(1024):
                        output.write("12 [workload] [STDOUT] '" + 'x' * 8192 + "'\n")
                    if mode == 'blocked-output-failures':
                        output.write('13 [antithesis_test_composer] [JSON] ' + json.dumps({'task_status': 'finished', 'command': 'test', 'command_return_code': 1}) + '\n')
                        output.write('14 [workload] [JSON] ' + json.dumps({'antithesis_assert': {'assert_type': 'always', 'hit': True, 'condition': False, 'message': 'late violation'}}))
            with log.open('a') as output:
                output.write(contents)
            record('compose_started', 'yes')
            if mode != 'waiting-setup':
                with log.open('a') as output:
                    output.write("13 [workload] [JSON] '{\"antithesis_setup\":{\"status\":\"complete\"}}'\n")
        elif script == 'fault_injector_update --unpause\n':
            record('faults_unpaused', 'yes')
            event('unpause')
            if mode == 'fault-injector-failure':
                sys.exit(1)
        elif 'composer_cgroup=' in script:
            event('composer-start')
            record('composer_starting', 'yes')
            if mode == 'readiness-blocked':
                while not (root / 'ready').exists():
                    time.sleep(0.05)
            if mode == 'readiness-failure':
                sys.exit('composer not ready')
            event('composer-ready')
            record('rollout_started', 'yes')
        elif script.startswith('/etc/fuzzing/add_entropy '):
            assert (root / 'rollout_started').exists()
            seed, force = script.split()[1:]
            assert 0 <= int(seed) < 2**64 and force == 'true'
            event('entropy ' + seed + ' ' + force)
            with (root / 'entropy_calls').open('a') as output:
                output.write(str(time.monotonic()) + ' ' + seed + ' ' + force + '\n')
        elif '--property=Result' in script:
            if mode == 'composer-failure':
                event('pause')
                sys.exit('composer failed fixture')
            if (root / 'composer_exit').exists():
                record('composer_completed', 'yes')
                print(json.dumps('completed'))
            else:
                print(json.dumps('running'))
    elif command.startswith('tar '):
        sys.stdin.buffer.read()
        if mode.endswith('blocked-upload'):
            record('upload_blocked', 'yes')
            while True:
                time.sleep(1)
    else:
        sys.exit('unexpected ssh command: ' + repr(args))
elif name == 'ssh-keygen':
    if mode == 'interrupt-before-boot':
        record('before_boot', Path.cwd())
        while True:
            time.sleep(1)
    private_key = Path(args[args.index('-f') + 1])
    public_key = Path(str(private_key) + '.pub')
    private_key.write_text('fixture private key\n')
    private_key.chmod(0o600)
    public_key.write_text('fixture public key\n')
    record('private_key_path', private_key)
    record('public_key_path', public_key)
elif name in ('qemu-system-x86_64', 'qemu-system-aarch64'):
    record('qemu_pid', os.getpid())
    record('qemu_tool', name)
    record('qemu_args', json.dumps(args))
    record('run_dir', Path.cwd())
    Path('boot.log').write_bytes(b'\x1b[18t\x1b[6nprivate boot console\n')
    Path('instrumentation.log').touch()
    if mode == 'boot-failure':
        Path('boot.log').write_text('fatal boot fixture\n')
        sys.exit(23)
    print(json.dumps({'QMP': {'version': {}, 'capabilities': []}}), flush=True)
    assert json.loads(sys.stdin.readline()) == {'execute': 'qmp_capabilities'}
    print(json.dumps({'return': {}}), flush=True)
    assert json.loads(sys.stdin.readline()) == {
        'execute': 'human-monitor-command',
        'arguments': {'command-line': 'info usernet'},
    }
    print(json.dumps({'return': (
        'Hub -1 (net0):\r\n'
        '  Protocol[State]    FD  Source Address  Port   Dest. Address  Port RecvQ SendQ\r\n'
        '  TCP[HOST_FORWARD]   3       127.0.0.1 34825       10.0.2.15    22     0     0\r\n'
    )}), flush=True)
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    record('qemu_child_pid', child.pid)
    def stop(signum, _frame):
        record('qemu_signal', signum)
        if signum == signal.SIGUSR1:
            time.sleep(0.1)
            with Path('instrumentation.log').open('a') as output:
                output.write("15 [workload] [STDOUT] 'last output before poweroff'\n")
        if child.poll() is None:
            try:
                child.terminate()
            except ProcessLookupError:
                pass
        child.wait(timeout=5)
        sys.exit(0)
    signal.signal(signal.SIGUSR1, stop)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with Path('instrumentation.log').open('a') as output:
        output.write("1 [boot] [JSON] '{\"antithesis_setup\":{\"status\":\"complete\"}}'\n")
        output.flush()
        if mode == 'fragmented-ready':
            record('marker_pending', 'yes')
            while not (root / 'emit_marker').exists():
                time.sleep(0.01)
        marker = "2 [antithesis/ssh-ready.json] [JSON] '{\"antithesis_host\":{\"status\":\"ssh_ready\"}}'\n"
        record('ssh_ready', 'yes')
        if mode == 'fragmented-ready':
            output.write(marker[:-1])
            output.flush()
            record('marker_partial', 'yes')
            while not (root / 'finish_marker').exists():
                time.sleep(0.01)
            output.write(marker[-1:])
        else:
            output.write(marker)
    while True:
        time.sleep(1)
else:
    sys.exit('unexpected tool: ' + name)
