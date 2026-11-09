"""One HTTP master plus one durable worker; fail together for host restart."""
import os
import signal
import subprocess
import sys
import threading
import time
from .gateway import Gateway


def application():
    return Gateway(os.environ.get('ANO_DATA_DIR', '/var/data/ano'), os.environ.get('ANO_GATEWAY_TOKEN', ''))


def main():
    os.umask(0o077)
    application()  # Validate secrets and persistent storage before opening a port.
    port = int(os.environ.get('PORT', '10000'))
    if not 1 <= port <= 65535:
        raise ValueError('Invalid PORT')
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    children = []
    failed = False
    try:
        children.append(subprocess.Popen([sys.executable, '-m', 'ano_worker', 'run'], start_new_session=True))
        children.append(subprocess.Popen([sys.executable, '-m', 'gunicorn',
            '--config', '/dev/null', '--bind', '0.0.0.0:' + str(port),
            '--workers', '1', '--worker-class', 'sync', '--timeout', '30',
            '--graceful-timeout', '10', '--worker-tmp-dir', '/tmp',
            '--limit-request-line', '2048', '--limit-request-fields', '30',
            '--limit-request-field_size', '4096', 'ano_worker.serve:application()'],
            start_new_session=True))
        while not stop.wait(0.1):
            if any(child.poll() is not None for child in children):
                failed = True
                break
    finally:
        # Signal complete groups so the HTTP master's children cannot be orphaned.
        for child in children:
            try: os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError: pass
        deadline = time.monotonic() + 15
        for child in children:
            try: child.wait(timeout=max(0.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                try: os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError: pass
                child.wait()
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
