import json
from pathlib import Path
import tempfile
import unittest
from ano_worker.__main__ import Worker

class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.worker = Worker(self.temp.name)
        self.worker.acquire()
    def tearDown(self):
        self.worker.close()
        self.temp.cleanup()
    def payload(self):
        return {'route':'api','kind':'text_report','text':'Exact punctuation.\nUnicode: café.'}
    def test_verified_readback(self):
        identifier=self.worker.submit(self.payload()); self.worker.tick()
        self.assertEqual(self.worker.store.get(identifier)['state'],'verified')
        self.assertEqual((Path(self.temp.name)/(identifier+'.txt')).read_text(),self.payload()['text'])
        self.assertEqual(len(self.worker.store.outputs(identifier)),1)
    def test_routes_and_kinds_fail_closed(self):
        for field,value in [('route','offline'),('route','local'),('kind','shell')]:
            payload=self.payload(); payload[field]=value
            with self.assertRaises(ValueError): self.worker.submit(payload)
    def test_single_worker(self):
        other=Worker(self.temp.name)
        with self.assertRaises(RuntimeError): other.acquire()
    def test_existing_file_preserved(self):
        identifier=self.worker.submit(self.payload()); path=Path(self.temp.name)/(identifier+'.txt');path.write_text('existing')
        self.worker.tick(); self.assertEqual(path.read_text(),'existing')
        self.assertEqual(self.worker.store.get(identifier)['state'],'paused')
    def test_restart_holds_interrupted_work(self):
        identifier=self.worker.submit(self.payload());self.worker.store.claim_next()
        self.worker.store.start_step(identifier,'text_report',True)
        self.worker.close(); self.worker=Worker(self.temp.name);self.worker.acquire()
        self.assertEqual(self.worker.store.get(identifier)['state'],'paused')
        self.assertIsNone(self.worker.tick())
        self.assertEqual(self.worker.store.steps(identifier)[0]['state'],'uncertain')
    def test_cancellation_survives_restart(self):
        identifier=self.worker.submit(self.payload());self.worker.store.claim_next();self.worker.store.request_cancel(identifier)
        self.worker.close();self.worker=Worker(self.temp.name);self.worker.acquire()
        self.assertEqual(self.worker.store.get(identifier)['state'],'cancelled')
    def test_queued_cancel_not_executed(self):
        identifier=self.worker.submit(self.payload());self.worker.store.request_cancel(identifier)
        self.assertIsNone(self.worker.tick())
    def test_tampered_queue_rejected(self):
        identifier=self.worker.store.enqueue(self.worker.store.project_for_chat(1),json.dumps({'route':'offline'}))
        self.worker.tick();self.assertEqual(self.worker.store.get(identifier)['state'],'failed')

if __name__=='__main__':unittest.main()
