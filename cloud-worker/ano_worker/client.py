"""Explicit API-route client. No redirects, no implicit cloud fallback."""
import hashlib
import json
import re
import urllib.request
import urllib.parse
from pathlib import Path

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

class Client:
    def __init__(self, base_url, token, route, *, test_loopback=False):
        parsed=urllib.parse.urlsplit(base_url)
        local=test_loopback and parsed.scheme=='http' and parsed.hostname=='127.0.0.1'
        if route!='api':raise ValueError('Remote work requires API route.')
        if not parsed.hostname or (parsed.scheme!='https' and not local) or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('','/'):
            raise ValueError('Use an HTTPS service origin.')
        if not isinstance(token,str) or len(token)<32:raise ValueError('Private token required.')
        self.base=base_url.rstrip('/');self.token=token
        self.opener=urllib.request.build_opener(NoRedirect)
    def request(self,path,payload=None,key=None):
        headers={'Authorization':'Bearer '+self.token}
        body=None
        if payload is not None:
            body=json.dumps(payload).encode();headers['Content-Type']='application/json'
        if key:headers['Idempotency-Key']=key
        req=urllib.request.Request(self.base+path,data=body,headers=headers)
        with self.opener.open(req,timeout=15) as response:
            raw=response.read(200001)
        if len(raw)>200000:raise ValueError('Response too large.')
        return json.loads(raw)
    def submit(self,text,request_id):
        return self.request('/v1/jobs',{'route':'api','kind':'text_report','text':text},request_id)
    def sync(self,job_id,directory):
        if not re.fullmatch('[a-f0-9]{32}',job_id):raise ValueError('Invalid job ID.')
        receipt=self.request('/v1/jobs/'+job_id+'/output')
        data=receipt['text'].encode()
        if receipt['job_id']!=job_id or len(data)!=receipt['bytes'] or hashlib.sha256(data).hexdigest()!=receipt['sha256']:
            raise ValueError('Receipt mismatch.')
        directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
        path=directory/(job_id+'.txt')
        with path.open('xb') as stream:stream.write(data)
        if path.read_bytes()!=data:raise ValueError('Local read-back failed.')
        return path
