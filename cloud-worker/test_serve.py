import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import urllib.error


class ServingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0)); self.port = sock.getsockname()[1]
        self.env = dict(os.environ, ANO_DATA_DIR=self.tmp.name, PORT=str(self.port), ANO_GATEWAY_TOKEN='test-only-' + 'x'*40)

    def start(self):
        proc = subprocess.Popen([sys.executable, '-m', 'ano_worker.serve'], env=self.env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self.stop, proc)
        for _ in range(100):
            if proc.poll() is not None: self.fail('Server exited during startup')
            try:
                urllib.request.urlopen('http://127.0.0.1:%d/' % self.port, timeout=.2)
            except urllib.error.HTTPError as err:
                if err.code == 401: return proc
            except OSError: pass
            time.sleep(.05)
        self.fail('Server did not become ready')

    def stop(self, proc):
        if proc.poll() is None:
            proc.terminate()
            try: proc.wait(timeout=20)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()

    def request(self, path, data=None):
        request = urllib.request.Request('http://127.0.0.1:%d%s' % (self.port,path),
            data=json.dumps(data).encode() if data else None,
            headers={'Authorization':'Bearer '+self.env['ANO_GATEWAY_TOKEN'],
                     'Content-Type':'application/json','Idempotency-Key':'serve-test-request-001'})
        with urllib.request.urlopen(request,timeout=5) as response: return json.load(response)

    def test_production_http_executes_and_survives_restart(self):
        proc=self.start()
        payload={'route':'api','kind':'text_report','text':'Exact report — with final period.'}
        job=self.request('/v1/jobs',payload)['job_id']
        for _ in range(100):
            status=self.request('/v1/jobs/'+job)
            if status['state']=='verified': break
            time.sleep(.05)
        self.assertEqual(status['state'],'verified')
        result=self.request('/v1/jobs/'+job+'/output')
        self.assertEqual(result['text'],payload['text'])
        self.stop(proc); self.assertEqual(proc.returncode,0)
        self.start()
        self.assertEqual(self.request('/v1/jobs',payload)['job_id'],job)
        self.assertEqual(self.request('/v1/jobs/'+job+'/output'),result)

    @unittest.skipUnless(sys.platform=='linux','Linux process supervision check')
    def test_worker_death_stops_http_and_supervisor(self):
        proc=self.start()
        child_pids=Path('/proc/%d/task/%d/children' % (proc.pid,proc.pid)).read_text().split()
        worker=next(int(pid) for pid in child_pids if b'ano_worker\x00run' in Path('/proc/'+pid+'/cmdline').read_bytes())
        os.kill(worker,signal.SIGKILL)
        self.assertEqual(proc.wait(timeout=20),1)
        with socket.socket() as sock:
            self.assertNotEqual(sock.connect_ex(('127.0.0.1',self.port)),0)

    def test_missing_token_fails_before_listening(self):
        self.env.pop('ANO_GATEWAY_TOKEN')
        result=subprocess.run([sys.executable,'-m','ano_worker.serve'],env=self.env,
                              capture_output=True,timeout=10)
        self.assertNotEqual(result.returncode,0)
        with socket.socket() as sock:
            self.assertNotEqual(sock.connect_ex(('127.0.0.1',self.port)),0)
