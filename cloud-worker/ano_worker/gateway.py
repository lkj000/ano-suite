"""Single-owner authenticated WSGI API; deploy behind HTTPS, never bare HTTP."""
import hashlib
import hmac
import json
import re
import uuid
from .__main__ import Worker, validate
from .jobs import JobStateError

class Gateway:
    def __init__(self, root, token):
        if not isinstance(token,str) or len(token)<32 or not token.isascii():
            raise ValueError('A private ASCII bearer token of at least 32 characters is required.')
        self.token=token
        self.worker=Worker(root)
        with self.worker.store._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS remote_requests (request_id TEXT PRIMARY KEY, digest TEXT NOT NULL, job_id TEXT NOT NULL REFERENCES work_jobs(id))')

    def submit(self, payload, request_id):
        validate(payload)
        if not re.fullmatch(r'[A-Za-z0-9_-]{16,80}',request_id):raise ValueError('Invalid request ID.')
        goal=json.dumps(payload,ensure_ascii=False,sort_keys=True)
        if len(goal)>64000:raise ValueError('Request too large.')
        digest=hashlib.sha256(goal.encode()).hexdigest()
        store=self.worker.store
        project=store.project_for_chat(1)
        with store._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT digest,job_id FROM remote_requests WHERE request_id=?',(request_id,)).fetchone()
            if old:
                if old['digest']!=digest:raise FileExistsError('Request ID already used with different content.')
                return old['job_id']
            if db.execute('SELECT count(*) FROM remote_requests').fetchone()[0]>=10000:
                raise OverflowError('Job retention capacity reached; operator maintenance required.')
            identifier=uuid.uuid4().hex;now=store._now()
            db.execute("INSERT INTO work_jobs(id,project_id,goal,state,created_at,updated_at,checkpoint) VALUES (?,?,?,'queued',?,?,?)",(identifier,project,goal,now,now,json.dumps({'requires_context':False})))
            db.execute('INSERT INTO remote_requests VALUES (?,?,?)',(request_id,digest,identifier))
        return identifier

    def __call__(self, env, start_response):
        def reply(code, data):
            body=json.dumps(data,ensure_ascii=False).encode()
            start_response(code,[('Content-Type','application/json'),('Content-Length',str(len(body))),('Cache-Control','no-store'),('X-Content-Type-Options','nosniff')])
            return [body]
        auth=env.get('HTTP_AUTHORIZATION','')
        if not isinstance(auth,str) or not hmac.compare_digest(auth.encode(),('Bearer '+self.token).encode()):
            return reply('401 Unauthorized',{'error':'Unauthorized'})
        try:
            method=env.get('REQUEST_METHOD');path=env.get('PATH_INFO','')
            if method=='POST' and path=='/v1/jobs':
                if env.get('CONTENT_TYPE','').split(';')[0]!='application/json':
                    return reply('415 Unsupported Media Type',{'error':'JSON required'})
                length=int(env.get('CONTENT_LENGTH','0'))
                if not 0<length<=64000:return reply('413 Content Too Large',{'error':'Request exceeds limit'})
                raw=env['wsgi.input'].read(length)
                if len(raw)!=length:raise ValueError('Incomplete request.')
                job=self.submit(json.loads(raw),env.get('HTTP_IDEMPOTENCY_KEY',''))
                return reply('202 Accepted',{'job_id':job})
            match=re.fullmatch(r'/v1/jobs/([a-f0-9]{32})(/cancel|/output)?',path)
            if not match:return reply('404 Not Found',{'error':'Not found'})
            identifier,action=match.groups();store=self.worker.store
            job=store.get(identifier)
            if action=='/cancel' and method=='POST':
                return reply('200 OK',{'job_id':identifier,'state':store.request_cancel(identifier)})
            if method!='GET':return reply('405 Method Not Allowed',{'error':'Method not allowed'})
            if action=='/output':
                outputs=store.outputs(identifier)
                if job['state']!='verified' or len(outputs)!=1:
                    return reply('409 Conflict',{'error':'No verified output'})
                output=outputs[0];path=self.worker.root/(identifier+'.txt')
                if path.is_symlink() or path.stat().st_size>32000:raise ValueError('Invalid output.')
                data=path.read_bytes()
                if len(data)!=output['bytes'] or hashlib.sha256(data).hexdigest()!=output['sha256']:
                    return reply('409 Conflict',{'error':'Output changed after verification'})
                return reply('200 OK',{'job_id':identifier,'text':data.decode('utf-8'),'sha256':output['sha256'],'bytes':len(data)})
            if action is not None:return reply('405 Method Not Allowed',{'error':'Method not allowed'})
            return reply('200 OK',{'job_id':identifier,'state':job['state'],'outputs':store.outputs(identifier)})
        except FileExistsError:
            return reply('409 Conflict',{'error':'Request ID content conflict'})
        except OverflowError:
            return reply('429 Too Many Requests',{'error':'Retention capacity reached'})
        except JobStateError:
            return reply('404 Not Found',{'error':'Job not found'})
        except (ValueError,UnicodeError,TypeError):
            return reply('400 Bad Request',{'error':'Invalid request'})
        except Exception:
            return reply('503 Service Unavailable',{'error':'Operation unavailable'})
