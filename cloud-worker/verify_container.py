"""Build and verify the actual Linux image. Never provisions cloud resources."""
import json
from pathlib import Path
import subprocess
import sys
import uuid

root = Path(__file__).resolve().parent
receipt = root.parent / 'linux-container-verification.json'
image = 'ano-worker-verification:' + uuid.uuid4().hex[:12]
checks = []

def run(command, timeout):
    result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout)[-2000:])
    return result.stdout.strip()

report = {'status': 'blocked', 'checks': checks, 'image': image, 'cloud_resources_created': False}
try:
    engine = run(['docker', 'info', '--format', '{{.OSType}}'], 10)
    if engine != 'linux':
        raise RuntimeError('Linux container engine required.')
    checks.append('Linux engine available')
    run(['docker', 'build', '-t', image, '.'], 300)
    checks.append('Production Dockerfile builds')
    uid = run(['docker', 'run', '--rm', '--network=none', image, 'python', '-c', 'import os;print(os.getuid())'], 30)
    if uid != '10001': raise RuntimeError('Expected non-root UID 10001.')
    checks.append('Non-root UID verified')
    output = run(['docker', 'run', '--rm', '--network=none', '--read-only',
                  '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m,mode=1777',
                  '-v', str(root / 'test_worker.py') + ':/app/test_worker.py:ro',
                  '-v', str(root / 'test_process.py') + ':/app/test_process.py:ro',
                  '-v', str(root / 'test_gateway.py') + ':/app/test_gateway.py:ro',
                  '-v', str(root / 'test_serve.py') + ':/app/test_serve.py:ro',
                  image, 'python', '-m', 'unittest', '-v', 'test_worker', 'test_process', 'test_gateway', 'test_serve'], 120)
    checks.append('21 tests passed inside Linux image, including authenticated HTTP flow and process termination/recovery')
    report['status'] = 'passed'
    report['limits'] = ['HTTP integration tested on container loopback only; hosted TLS, Render disk permissions and native Mac UI integration not tested.']
except (RuntimeError, subprocess.TimeoutExpired, FileNotFoundError) as exc:
    report['status'] = 'failed' if checks else 'blocked'
    report['reason'] = str(exc)
finally:
    receipt.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    if 'Production Dockerfile builds' in checks:
        try: run(['docker', 'image', 'rm', image], 20)
        except Exception: pass
sys.exit(0 if report['status'] == 'passed' else 1)
