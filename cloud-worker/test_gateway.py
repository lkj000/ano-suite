import io,json,secrets,tempfile,threading,unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from wsgiref.simple_server import make_server,WSGIRequestHandler
from ano_worker.gateway import Gateway
from ano_worker.client import Client

class Quiet(WSGIRequestHandler):
    def log_message(self,*args):pass

class GatewayTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.token=secrets.token_urlsafe(32)
        self.api=Gateway(self.temp.name,self.token)
    def tearDown(self):self.temp.cleanup()
    def payload(self):return {'route':'api','kind':'text_report','text':'Remote proof.\n'}
    def call(self,path='/v1/jobs',method='POST',payload=None,token=None,key='request-00000001'):
        raw=json.dumps(self.payload() if payload is None else payload).encode(); status=[]
        env={'PATH_INFO':path,'REQUEST_METHOD':method,'CONTENT_TYPE':'application/json','CONTENT_LENGTH':str(len(raw)),'wsgi.input':io.BytesIO(raw),'HTTP_AUTHORIZATION':'Bearer '+(self.token if token is None else token),'HTTP_IDEMPOTENCY_KEY':key}
        body=b''.join(self.api(env,lambda code,headers:status.append(code)))
        return int(status[0].split()[0]),json.loads(body)
    def test_unauthorized_has_no_side_effect(self):
        self.assertEqual(self.call(token='wrong')[0],401)
        self.assertEqual(self.api.worker.store.list_jobs(),[])
    def test_idempotency_survives_restart_and_conflicts(self):
        first=self.call()[1]['job_id'];self.api=Gateway(self.temp.name,self.token)
        self.assertEqual(self.call()[1]['job_id'],first)
        payload=self.payload();payload['text']='Different';self.assertEqual(self.call(payload=payload)[0],409)
        self.assertEqual(len(self.api.worker.store.list_jobs()),1)
    def test_concurrent_duplicate_submission_is_atomic(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            ids=list(pool.map(lambda _:self.api.submit(self.payload(),'concurrent-00001'),range(8)))
        self.assertEqual(len(set(ids)),1)
    def test_offline_and_arbitrary_tools_rejected(self):
        for field,value in [('route','offline'),('kind','shell')]:
            payload=self.payload();payload[field]=value;self.assertEqual(self.call(payload=payload)[0],400)
    def test_changed_output_not_certified(self):
        job=self.call()[1]['job_id'];self.api.worker.acquire()
        try:self.api.worker.tick()
        finally:self.api.worker.close()
        (Path(self.temp.name)/(job+'.txt')).write_text('changed')
        self.assertEqual(self.call('/v1/jobs/'+job+'/output','GET')[0],409)
    def test_cancel_pending_job(self):
        job=self.call()[1]['job_id']
        self.assertEqual(self.call('/v1/jobs/'+job+'/cancel')[1]['state'],'cancelled')
        self.assertEqual(self.call('/v1/jobs/'+job+'/output','GET')[0],409)
    def test_client_rejects_insecure_or_offline_dispatch(self):
        for url,route in [('http://example.com','api'),('https://example.com','offline'),('https://example.com/path','api')]:
            with self.assertRaises(ValueError):Client(url,self.token,route)
    def test_real_http_submit_execute_sync(self):
        server=make_server('127.0.0.1',0,self.api,handler_class=Quiet)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            client=Client('http://127.0.0.1:'+str(server.server_port),self.token,'api',test_loopback=True)
            job=client.submit('UTF-8 café.\n','http-request-0001')['job_id']
            self.api.worker.acquire()
            try:self.api.worker.tick()
            finally:self.api.worker.close()
            path=client.sync(job,Path(self.temp.name)/'mac-receipts')
            self.assertEqual(path.read_text(),'UTF-8 café.\n')
            with self.assertRaises(FileExistsError):client.sync(job,path.parent)
        finally:server.shutdown();server.server_close();thread.join()

if __name__=='__main__':unittest.main()
