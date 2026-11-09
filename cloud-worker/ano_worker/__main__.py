import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import threading
from .jobs import JobStore


def validate(payload):
    if not isinstance(payload, dict) or set(payload) != {'route', 'kind', 'text'}:
        raise ValueError('Expected only route, kind and text.')
    if payload['route'] != 'api':
        raise ValueError('Cloud dispatch requires API route; Offline and Local stay on device.')
    if payload['kind'] != 'text_report':
        raise ValueError('Unsupported task kind.')
    if not isinstance(payload['text'], str) or not payload['text'] or len(payload['text'].encode('utf-8')) > 32000:
        raise ValueError('Text must contain 1–32000 UTF-8 bytes.')
    return payload


class Worker:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.store = JobStore(self.root / 'jobs.sqlite3')
        self.lock = None

    def acquire(self):
        self.lock = open(self.root / 'worker.lock', 'a')
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.lock.close()
            self.lock = None
            raise RuntimeError('Another worker owns this data directory.')
        # Preserve cancellation across restart; uncertain writes are never replayed.
        with self.store._db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE work_steps SET state='uncertain' WHERE state='running'")
            db.execute("UPDATE work_jobs SET state='cancelled',note='Cancellation preserved after restart' WHERE state='cancelling'")
        self.store.recover_interrupted()

    def close(self):
        if self.lock:
            self.lock.close()
            self.lock = None

    def submit(self, payload):
        validate(payload)
        project = self.store.project_for_chat(1)
        return self.store.enqueue(project, json.dumps(payload, ensure_ascii=False))

    def tick(self):
        if self.lock is None:
            raise RuntimeError('Worker lock required.')
        job = self.store.claim_next()
        if job is None:
            return None
        step = None
        try:
            payload = validate(json.loads(job['goal']))
            if job['checkpoint'].get('requires_reinspection'):
                self.store.pause(job['id'])
                return job['id']
            step = self.store.start_step(job['id'], 'text_report', side_effect=True)
            path = self.root / (job['id'] + '.txt')
            expected = payload['text'].encode('utf-8')
            # Exclusive creation refuses existing files and symlinks. No caller-supplied path.
            with path.open('xb') as stream:
                stream.write(expected)
                stream.flush()
                os.fsync(stream.fileno())
            directory = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            actual = path.read_bytes()
            if actual != expected:
                raise ValueError('Read-back differs from supplied text.')
            self.store.finish_step(step, 'returned')
            self.store.record_output(step, {'path': path.name, 'sha256': hashlib.sha256(actual).hexdigest(), 'bytes': len(actual), 'format': 'txt'})
            state = self.store.get(job['id'])['state']
            self.store.finish(job['id'], 'cancelled' if state == 'cancelling' else 'verified',
                              {'passed': True, 'references': [path.name], 'scope': 'Exact byte read-back only; not AI task completion.'})
        except Exception:
            # A write could have happened before an error. Hold it for inspection.
            if step and self.store.steps(job['id'])[-1]['state'] == 'running':
                self.store.finish_step(step, 'uncertain')
            if self.store.get(job['id'])['state'] == 'cancelling':
                self.store.finish(job['id'], 'cancelled')
            elif step:
                self.store.pause(job['id'])
            else:
                self.store.finish(job['id'], 'failed')
        return job['id']


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser()
    parser.add_argument('--data-dir', default=os.environ.get('ANO_DATA_DIR', '/var/data/ano'))
    parser.add_argument('command', choices=['run', 'once', 'submit', 'status', 'cancel'])
    parser.add_argument('value', nargs='?')
    args = parser.parse_args()
    worker = Worker(args.data_dir)
    if args.command == 'submit':
        with open(args.value, encoding='utf-8') as stream:
            print(worker.submit(json.load(stream)))
    elif args.command == 'status':
        # Never include job text in operational logs.
        print(json.dumps([{k: job[k] for k in ('id', 'state', 'updated_at')} for job in worker.store.list_jobs()]))
    elif args.command == 'cancel':
        print(worker.store.request_cancel(args.value))
    else:
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        worker.acquire()
        try:
            while not stop.is_set():
                identifier = worker.tick()
                if identifier:
                    print(json.dumps({'job': identifier, 'state': worker.store.get(identifier)['state']}), flush=True)
                if args.command == 'once':
                    break
                stop.wait(2)
        finally:
            worker.close()

if __name__ == '__main__':
    main()
