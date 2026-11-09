import subprocess,sys,tempfile,time,unittest
from pathlib import Path
from ano_worker.__main__ import Worker

class ProcessTests(unittest.TestCase):
    def test_stop_restart_preserves_verified_output(self):
        with tempfile.TemporaryDirectory() as root:
            worker=Worker(root)
            job=worker.submit({'route':'api','kind':'text_report','text':'One exact report.\n'})
            command=[sys.executable,'-m','ano_worker','--data-dir',root]
            process=subprocess.Popen(command+['run'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            try:
                deadline=time.monotonic()+8
                while worker.store.get(job)['state']!='verified' and time.monotonic()<deadline:time.sleep(.05)
                self.assertEqual(worker.store.get(job)['state'],'verified')
                process.terminate();out,err=process.communicate(timeout=5)
                self.assertEqual(process.returncode,0,err)
                path=Path(root)/(job+'.txt');mtime=path.stat().st_mtime_ns
                subprocess.run(command+['once'],check=True,capture_output=True,timeout=5)
                self.assertEqual(path.stat().st_mtime_ns,mtime)
                self.assertEqual(worker.store.get(job)['attempts'],1)
            finally:
                if process.poll() is None:process.kill()
                process.communicate(timeout=5)

    def test_killed_owner_releases_lock_and_holds_work(self):
        with tempfile.TemporaryDirectory() as root:
            worker=Worker(root)
            job=worker.submit({'route':'api','kind':'text_report','text':'Do not replay.'})
            code="import sys,time;from ano_worker.__main__ import Worker;w=Worker(sys.argv[1]);w.acquire();j=w.store.claim_next();w.store.start_step(j['id'],'text_report',True);time.sleep(30)"
            process=subprocess.Popen([sys.executable,'-c',code,root])
            try:
                deadline=time.monotonic()+8
                while not worker.store.steps(job) and time.monotonic()<deadline:time.sleep(.05)
                self.assertTrue(worker.store.steps(job))
                process.kill();process.wait(timeout=5)
                subprocess.run([sys.executable,'-m','ano_worker','--data-dir',root,'once'],check=True,capture_output=True,timeout=5)
                self.assertEqual(worker.store.get(job)['state'],'paused')
                self.assertEqual(worker.store.steps(job)[0]['state'],'uncertain')
                self.assertFalse((Path(root)/(job+'.txt')).exists())
            finally:
                if process.poll() is None:process.kill()
                process.wait(timeout=5)

if __name__=='__main__':unittest.main()
