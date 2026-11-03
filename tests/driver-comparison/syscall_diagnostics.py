"""Optional strace attachment for comparison diagnostics, never confirmation."""
from collections import Counter
import datetime
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import threading


def network_syscalls(path, measurement):
    start = datetime.datetime.fromisoformat(measurement['start'].replace('Z', '+00:00')).timestamp()
    end = datetime.datetime.fromisoformat(measurement['drain_end'].replace('Z', '+00:00')).timestamp()
    completed = measurement['completed']
    counts, transferred, batches = Counter(), Counter(), Counter()
    pending = {}
    malformed = 0
    header = re.compile(r'^\s*(\d+)\s+(\d+\.\d+)\s+(.*)$')
    call = re.compile(r'^(\w+)\(.*?\)\s+=\s+(-?\d+)')
    resumed = re.compile(r'^<\.\.\. (\w+) resumed>(.*)$')
    with path.open() as lines:
        for line in lines:
            match = header.match(line)
            if not match:
                continue
            tid, stamp, body = match.groups()
            stamp = float(stamp)
            if '<unfinished ...>' in body:
                pending[tid] = (stamp, body.replace('<unfinished ...>', ''))
                continue
            continuation = resumed.match(body)
            if continuation:
                previous = pending.pop(tid, None)
                if previous is None:
                    continue  # Attachment may begin during an existing syscall.
                _, prefix = previous
                body = prefix + continuation.group(2)
            else:
                elapsed = re.search(r'<(\d+\.\d+)>\s*$', body)
                if elapsed:
                    stamp += float(elapsed.group(1))
            if not start <= stamp <= end or not re.search(r'<TCP(?:v6)?:\[', body):
                continue
            match = call.match(body)
            if not match:
                malformed += 1
                continue
            name, result = match.group(1), int(match.group(2))
            counts[name] += 1
            if result > 0:
                transferred[name] += result
                if name in ('write', 'writev', 'sendto', 'sendmsg'):
                    batches[result] += 1
    if not completed or not counts or malformed:
        raise ValueError('missing completions/network syscalls or malformed trace records: ' + str(malformed))
    return dict(scope='Network syscall completions during measured phase through drain; tracing perturbs execution',
                completed=completed, syscall_counts=dict(counts), transferred_bytes=dict(transferred),
                syscalls_per_op={name: count / completed for name, count in counts.items()},
                bytes_per_syscall={name: transferred[name] / count for name, count in counts.items()},
                write_batch_bytes=dict(sorted(batches.items())), malformed_records=malformed)


class Trace:
    """Attach only to this coordinator's selected, directly spawned client."""
    def __init__(self, binary, output, cpus='2,18'):
        self.binary = Path(binary).resolve()
        self.output = Path(output)
        self.cpus = cpus
        self.stop = threading.Event()
        self.process = None
        self.error = None
        self.pid = None
        self.args = None

    def __enter__(self):
        self.thread = threading.Thread(target=self.collect, name='syscall-diagnostics')
        self.thread.start()
        return self

    def collect(self):
        try:
            children = Path(f'/proc/{os.getpid()}/task/{os.getpid()}/children')
            while not self.stop.wait(.002):
                for pid in children.read_text().split():
                    try:
                        if Path(f'/proc/{pid}/exe').resolve() != self.binary:
                            continue
                    except OSError:
                        continue
                    self.pid = int(pid)
                    self.args = ['taskset', '-c', self.cpus, 'sudo', '-n', 'strace', '-f', '-qq', '-ttt', '-T',
                                 '-s', '0', '-yy', '-e', 'trace=read,readv,recvfrom,recvmsg,write,writev,sendto,sendmsg',
                                 '-o', str(self.output), '-p', pid]
                    with self.output.with_suffix('.stderr').open('x') as stderr:
                        self.process = subprocess.Popen(self.args, stdin=subprocess.DEVNULL,
                                                        stdout=subprocess.DEVNULL, stderr=stderr,
                                                        start_new_session=True)
                        while not self.stop.wait(.05) and self.process.poll() is None:
                            pass
                        if self.process.poll() is None:
                            subprocess.run(['sudo', '-n', 'kill', '-INT', '--', '-' + str(self.process.pid)],
                                           check=True, capture_output=True, timeout=10)
                        self.process.wait(timeout=10)
                        if self.process.returncode not in (0, -signal.SIGINT, 128 + signal.SIGINT):
                            raise RuntimeError('strace failed: ' + str(self.process.returncode))
                    return
            raise RuntimeError('selected benchmark child was never found')
        except BaseException as error:
            self.error = str(error)
            if self.process is not None and self.process.poll() is None:
                subprocess.run(['sudo', '-n', 'kill', '-KILL', '--', '-' + str(self.process.pid)],
                               capture_output=True, timeout=10)
                self.process.wait(timeout=10)

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            self.error = 'strace collector did not finish'
        self.output.with_suffix('.manifest.json').write_text(json.dumps(dict(
            binary=str(self.binary), client_pid=self.pid, args=self.args,
            monitor_cpus=self.cpus, returncode=self.process.poll() if self.process else None,
            error=self.error, payload_capture_bytes=0, performance_confirmation=False), indent=2) + '\n')
