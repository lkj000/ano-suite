"""Exercise durable state across disposable Linux container replacements."""
import json
from pathlib import Path
import subprocess
import sys
import time
import uuid

root = Path(__file__).resolve().parent
suffix = uuid.uuid4().hex[:12]
image = 'ano-volume-test:' + suffix
volume = 'ano-volume-test-' + suffix
container = 'ano-interruption-test-' + suffix
checks = []
report = {'status': 'blocked', 'checks': checks, 'cloud_resources_created': False}
created = {'image': False, 'volume': False, 'container': False}

def run(args, timeout=30):
    result = subprocess.run(['docker', *args], cwd=root, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout)[-1500:])
    return result.stdout.strip()

def invoke(*args):
    return run(['run', '--rm', '--network=none', '--read-only', '-v', volume + ':/var/data/ano', image, *args])

def python(code):
    return invoke('python', '-c', code)

try:
    if run(['info', '--format', '{{.OSType}}'], 10) != 'linux':
        raise RuntimeError('Linux engine required.')
    checks.append('Linux engine available')
    run(['build', '-t', image, '.'], 300); created['image'] = True
    run(['volume', 'create', volume]); created['volume'] = True
    # Docker initializes an empty volume from the image directory, including ownership.
    uid = python("import os;from pathlib import Path;p=Path('/var/data/ano/probe');p.write_text('ok');print(os.getuid());p.unlink()")
    if uid != '10001': raise RuntimeError('Volume write must run as UID 10001.')
    checks.append('Fresh named volume writable by non-root image user')
    job = python("from ano_worker.__main__ import Worker;print(Worker('/var/data/ano').submit({'route':'api','kind':'text_report','text':'Persistent report.\\n'}))")
    invoke('python', '-m', 'ano_worker', 'once')
    result = json.loads(python("import json;from ano_worker.__main__ import Worker;w=Worker('/var/data/ano');j="+repr(job)+";print(json.dumps({'state':w.store.get(j)['state'],'attempts':w.store.get(j)['attempts'],'text':(w.root/(j+'.txt')).read_text(),'outputs':len(w.store.outputs(j))}))"))
    assert result == {'state': 'verified', 'attempts': 1, 'text': 'Persistent report.\n', 'outputs': 1}, result
    invoke('python', '-m', 'ano_worker', 'once')
    assert json.loads(python("import json;from ano_worker.__main__ import Worker;print(json.dumps(Worker('/var/data/ano').store.get("+repr(job)+")))"))['attempts'] == 1
    checks.append('Queued job, exact output and receipt survive separate container instances without replay')
    pending = python("from ano_worker.__main__ import Worker;print(Worker('/var/data/ano').submit({'route':'api','kind':'text_report','text':'Interrupted output.'}))")
    code = "import time;from ano_worker.__main__ import Worker;w=Worker('/var/data/ano');w.acquire();j=w.store.claim_next();w.store.start_step(j['id'],'text_report',True);(w.root/(j['id']+'.txt')).write_text('partial');print('write-dispatched',flush=True);time.sleep(120)"
    run(['run', '-d', '--name', container, '--network=none', '--read-only', '-v', volume + ':/var/data/ano', image, 'python', '-c', code]);created['container'] = True
    deadline = time.monotonic() + 10
    while 'write-dispatched' not in run(['logs', container]):
        if time.monotonic() > deadline: raise RuntimeError('Interruption fixture did not start.')
        time.sleep(.1)
    run(['kill', '--signal=KILL', container])
    invoke('python', '-m', 'ano_worker', 'once')
    result = json.loads(python("import json;from ano_worker.__main__ import Worker;w=Worker('/var/data/ano');j="+repr(pending)+";print(json.dumps({'state':w.store.get(j)['state'],'step':w.store.steps(j)[0]['state'],'text':(w.root/(j+'.txt')).read_text(),'outputs':len(w.store.outputs(j))}))"))
    assert result == {'state': 'paused', 'step': 'uncertain', 'text': 'partial', 'outputs': 0}, result
    checks.append('SIGKILL with a dispatched write preserves partial output and holds job uncertain on replacement')
    report['status'] = 'passed'
except (RuntimeError, AssertionError, subprocess.TimeoutExpired, FileNotFoundError) as exc:
    report['status'] = 'failed' if checks else 'blocked'
    report['reason'] = str(exc)
finally:
    failures = []
    for key, args in [('container',['rm','-f',container]),('volume',['volume','rm',volume]),('image',['image','rm',image])]:
        if created[key]:
            try: run(args)
            except Exception as exc: failures.append(key + ': ' + str(exc))
    report['cleanup_errors'] = failures
    report['limits'] = ['Local Docker named-volume test only; Render mount ownership not verified.', 'No remote submission, model inference or Mac synchronization tested.']
    (root.parent/'linux-volume-verification.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
sys.exit(0 if report['status']=='passed' and not report['cleanup_errors'] else 1)
