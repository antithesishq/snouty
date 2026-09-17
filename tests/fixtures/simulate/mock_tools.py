import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

root = Path(__file__).resolve().parent.parent
args = sys.argv[1:]
name = Path(sys.argv[0]).name
mode = (root / 'mode').read_text()

def record(name, value):
    (root / name).write_text(str(value))

if name == 'docker-compose':
    if 'version' in args:
        print('2.39.0')
    elif 'config' in args:
        print(json.dumps({'services': {'workload': {'image': 'workload:test'}}}))
    else:
        sys.exit('unexpected compose command: ' + repr(args))
elif name == 'docker':
    if args[:2] == ['image', 'inspect']:
        if args[2] == 'guest:test' and mode in ('missing-guest', 'pull-failure') and not (root / 'pulled').exists():
            sys.exit('No such image: guest:test')
        if args[2] != 'workload:test':
            record('guest_image', args[2])
        print(json.dumps([{'Id': 'sha256:' + 'a' * 64, 'Architecture': 'amd64'}]))
    elif args[0] == 'cp':
        Path(args[-1]).write_bytes(b'guest ISO fixture')
    elif args[0] == 'save':
        Path(args[args.index('--output') + 1]).write_bytes(b'workload archive')
    elif args[0] == 'pull':
        record('pull_args', json.dumps(args))
        if mode == 'pull-failure':
            sys.exit('registry denied fixture')
        record('pulled', 'guest:test')
    elif args[0] in ('create', 'rm'):
        print('fixture-container')
    else:
        sys.exit('unexpected engine command: ' + repr(args))
elif name == 'ssh':
    assert args[0] == '-F', args
    config = Path(args[1])
    record('ssh_config', config.read_text())
    record('key_mode', oct((config.parent / 'guest-dev-key').stat().st_mode & 0o777))
    if args[2:] == ['-o', 'LogLevel=ERROR', '-tt', 'guest_vm']:
        record('shell_opened', 'yes')
        print('guest shell ready', flush=True)
        commands = []
        for line in sys.stdin:
            command = line.strip()
            commands.append(command)
            record('shell_input', '\n'.join(commands))
            if command == 'whoami':
                print('root', flush=True)
            elif command == 'poweroff':
                record('guest_poweroff', 'yes')
                os.kill(int((root / 'qemu_pid').read_text()), signal.SIGTERM)
                print('Connection to 127.0.0.1 closed.', file=sys.stderr)
                sys.exit(255)
            else:
                sys.exit('unexpected shell input: ' + repr(command))
        sys.exit(0)
    assert args[2] == 'guest_vm', args
    command = args[3]
    if command == 'true':
        pass
    elif command == 'bash -s':
        script = sys.stdin.read()
        record('batch_script', script)
        if 'up -d' in script:
            record('restart_mode', script.splitlines()[0])
            log = Path.cwd() / 'instrumentation.log'
            log.write_text("11.2 [workload] [JSON] '{\"antithesis_assert\":{\"message\":\"balance stays positive\",\"assert_type\":\"always\",\"must_hit\":true,\"hit\":true,\"condition\":false}}'\n12.0 [workload] [STDOUT] 'still running after assertion'\n")
            if mode == 'malformed-json':
                log.write_text("12.0 [workload] [JSON] '{broken json}'\n")
            if mode in ('supervisor-failure', 'clean-once', 'missing-guest', 'default-image'):
                log.write_text('12.0 [workload] [STDOUT] \'workload running\'\n')
            if mode.startswith('blocked-output'):
                with log.open('w') as output:
                    for _ in range(1024):
                        output.write("12 [workload] [STDOUT] '" + 'x' * 8192 + "'\n")
                    if mode == 'blocked-output-failures':
                        output.write('13 [antithesis_test_composer] [JSON] ' + json.dumps({'task_status': 'finished', 'command': 'test', 'command_return_code': 1}) + '\n')
                        output.write('14 [workload] [JSON] ' + json.dumps({'antithesis_assert': {'assert_type': 'always', 'hit': True, 'condition': False, 'message': 'late violation'}}))
            record('rollout_started', 'yes')
        elif 'podman image exists' in script:
            print('[]')
        elif '--property=Result' in script and mode == 'supervisor-failure':
            sys.exit('supervisor failed fixture')
    elif command.startswith('tar ') or command == 'podman load':
        sys.stdin.buffer.read()
    else:
        sys.exit('unexpected ssh command: ' + repr(args))
elif name == 'qemu-system-x86_64':
    record('qemu_pid', os.getpid())
    record('qemu_args', json.dumps(args))
    record('run_dir', Path.cwd())
    Path('boot.log').write_bytes(b'\x1b[18t\x1b[6nprivate boot console\n')
    Path('instrumentation.log').touch()
    if mode == 'boot-failure':
        Path('boot.log').write_text('fatal boot fixture\n')
        sys.exit(23)
    if mode == 'startup-timeout':
        while True:
            time.sleep(1)
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    record('qemu_child_pid', child.pid)
    def stop(signum, _frame):
        record('qemu_signal', signum)
        if child.poll() is None:
            try:
                child.terminate()
            except ProcessLookupError:
                pass
        child.wait(timeout=5)
        sys.exit(0)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    listener = socket.socket(socket.AF_UNIX)
    listener.bind('qmp.sock')
    listener.listen(1)
    with Path('boot.log').open('a') as output:
        output.write('Press [Tab] to edit options\n')
    connection, _ = listener.accept()
    with connection.makefile('rwb', buffering=0) as stream:
        stream.write(b'{"QMP":{}}\n')
        for line in stream:
            request = json.loads(line)
            with (root / 'qmp_requests').open('a') as output:
                output.write(request['execute'] + '\n')
            stream.write(b'{"return":{}}\n')
    while True:
        time.sleep(1)
else:
    sys.exit('unexpected tool: ' + name)
