"""Local persistent work ledger. Internal worker API, never a model-callable tool.

This stores progress; it does not execute commands or certify model statements.
An interrupted write is uncertain and is never automatically replayed.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import json
import re
import sqlite3
import uuid


TERMINAL = {'answered', 'verified', 'failed', 'cancelled'}


class JobStateError(ValueError):
    pass


class JobStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS work_projects (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS work_jobs (
                    id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES work_projects(id),
                    goal TEXT NOT NULL, chat_id INTEGER, request_message_id INTEGER, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                    checkpoint TEXT NOT NULL DEFAULT '{}', note TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS work_steps (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES work_jobs(id),
                    tool TEXT NOT NULL, side_effect INTEGER NOT NULL, state TEXT NOT NULL,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS work_queue ON work_jobs(state, created_at);
                CREATE TABLE IF NOT EXISTS work_outputs (
                    step_id TEXT NOT NULL REFERENCES work_steps(id), path TEXT NOT NULL,
                    sha256 TEXT NOT NULL, bytes INTEGER NOT NULL, format TEXT NOT NULL,
                    PRIMARY KEY(step_id,path));
            ''')
            columns = {row[1] for row in db.execute('PRAGMA table_info(work_jobs)')}
            if 'request_message_id' not in columns:
                db.execute('ALTER TABLE work_jobs ADD COLUMN request_message_id INTEGER')

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _now():
        return datetime.now(timezone.utc).isoformat(timespec='microseconds')

    @staticmethod
    def _text(value, limit):
        if not isinstance(value, str) or not value.strip() or len(value) > limit:
            raise ValueError(f'Expected non-empty text up to {limit} characters.')
        return value

    @staticmethod
    def _row(db, identifier):
        row = db.execute('SELECT * FROM work_jobs WHERE id=?', (identifier,)).fetchone()
        if row is None:
            raise JobStateError('That job does not exist.')
        return row

    def create_project(self, name):
        name = self._text(name, 120)
        identifier = uuid.uuid4().hex
        with self._db() as db:
            db.execute('INSERT INTO work_projects VALUES (?,?,?)', (identifier, name, self._now()))
        return identifier

    def project_for_chat(self, chat_id):
        if type(chat_id) is not int or chat_id < 1:
            raise ValueError('A project chat needs a positive chat identifier.')
        identifier = 'chat-' + str(chat_id)
        with self._db() as db:
            db.execute('INSERT OR IGNORE INTO work_projects VALUES (?,?,?)',
                       (identifier, 'Chat ' + str(chat_id), self._now()))
        return identifier

    def enqueue(self, project_id, goal, chat_id=None, request_message_id=None, requires_context=False):
        goal = self._text(goal, 64000)
        if chat_id is not None and type(chat_id) is not int:
            raise ValueError('chat_id must be an integer or absent.')
        if request_message_id is not None and (type(request_message_id) is not int or request_message_id < 1):
            raise ValueError('request_message_id must be a positive integer or absent.')
        if type(requires_context) is not bool:
            raise ValueError('requires_context must be boolean.')
        identifier, now = uuid.uuid4().hex, self._now()
        with self._db() as db:
            db.execute('INSERT INTO work_jobs(id,project_id,goal,chat_id,request_message_id,state,created_at,updated_at,checkpoint) '
                       "VALUES (?,?,?,?,?,'queued',?,?,?)", (identifier, project_id, goal, chat_id, request_message_id, now, now,
                       json.dumps({'requires_context': requires_context})))
        return identifier

    @staticmethod
    def _resume_blocker(db, row):
        from .work_ledger import READ_ONLY
        if row['state'] != 'paused':
            return 'Only a paused task can resume.'
        checkpoint = json.loads(row['checkpoint'])
        if 'requires_context' not in checkpoint:
            return 'This saved task predates recovery context tracking. Reissue the command with fresh context.'
        if checkpoint['requires_context'] is not False:
            return 'Capture fresh screen or activity context and issue a new command.'
        steps = db.execute('SELECT tool,side_effect FROM work_steps WHERE job_id=?', (row['id'],)).fetchall()
        changed = any(step['side_effect'] or step['tool'] not in READ_ONLY for step in steps)
        if changed:
            return 'An action may already have changed the destination. Inspect it before issuing a continuation; automatic replay is held.'
        return ''

    def recovery_options(self, identifier):
        with self._db() as db:
            row = self._row(db, identifier)
            reason = self._resume_blocker(db, row)
        return {'can_resume': not bool(reason), 'recovery_note': reason or 'No changing action was dispatched. Reads may run again.'}

    def resume_without_changes(self, identifier):
        """Explicitly resume only when no changing tool was ever dispatched.

        Do not infer safety from a model answer, failed receipt or resolved write.
        The check and transition share a write lock, so another worker cannot race it.
        """
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            reason = self._resume_blocker(db, row)
            if reason:
                raise JobStateError(reason)
            checkpoint = json.loads(row['checkpoint'])
            checkpoint.pop('requires_reinspection', None)
            db.execute("UPDATE work_steps SET state='failed',updated_at=? WHERE job_id=? AND side_effect=0 AND state IN ('running','uncertain')",
                       (self._now(), identifier))
            db.execute("UPDATE work_jobs SET state='queued',checkpoint=?,note=?,revision=revision+1,updated_at=? WHERE id=?",
                       (json.dumps(checkpoint), 'Resumed explicitly; no changing action was dispatched in earlier attempts.', self._now(), identifier))

    def get(self, identifier):
        with self._db() as db:
            result = dict(self._row(db, identifier))
        result['checkpoint'] = json.loads(result['checkpoint'])
        return result

    def list_jobs(self, project_id=None):
        with self._db() as db:
            rows = db.execute('SELECT id,project_id,chat_id,goal,state,revision,updated_at FROM work_jobs '
                              + ('WHERE project_id=? ' if project_id else '') + 'ORDER BY created_at',
                              (project_id,) if project_id else ()).fetchall()
        return [dict(row) for row in rows]

    def claim_next(self):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT id FROM work_jobs WHERE state='queued' ORDER BY created_at,id LIMIT 1").fetchone()
            if row is None:
                return None
            db.execute("UPDATE work_jobs SET state='running', attempts=attempts+1, revision=revision+1, updated_at=? WHERE id=?",
                       (self._now(), row['id']))
            identifier = row['id']
        return self.get(identifier)

    def save_checkpoint(self, identifier, checkpoint, expected_revision):
        if not isinstance(checkpoint, dict):
            raise ValueError('Checkpoint must be an object.')
        value = json.dumps(checkpoint, ensure_ascii=False, allow_nan=False)
        if len(value) > 64000:
            raise ValueError('Checkpoint exceeds its size limit.')
        with self._db() as db:
            changed = db.execute("UPDATE work_jobs SET checkpoint=?,revision=revision+1,updated_at=? "
                                 "WHERE id=? AND state='running' AND revision=?",
                                 (value, self._now(), identifier, expected_revision)).rowcount
            if not changed:
                raise JobStateError('Job changed or is not running; stale checkpoint refused.')
        return self.get(identifier)

    def start_step(self, identifier, tool, side_effect=False):
        if not isinstance(tool, str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', tool):
            raise ValueError('Tool name must be an identifier, never user content.')
        if type(side_effect) is not bool:
            raise ValueError('side_effect must be boolean.')
        step, now = uuid.uuid4().hex, self._now()
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self._row(db, identifier)['state'] != 'running':
                raise JobStateError('No new steps can start in this job state.')
            db.execute("INSERT INTO work_steps VALUES (?,?,?,?,'running',?,?)",
                       (step, identifier, tool, int(side_effect), now, now))
        return step

    def finish_step(self, step, state):
        if state not in ('verified', 'failed', 'uncertain', 'returned'):
            raise ValueError('Invalid step result.')
        with self._db() as db:
            changed = db.execute("UPDATE work_steps SET state=?,updated_at=? WHERE id=? AND state='running'",
                                 (state, self._now(), step)).rowcount
            if not changed:
                raise JobStateError('Step is missing or already resolved; result was not overwritten.')

    def steps(self, identifier):
        with self._db() as db:
            self._row(db, identifier)
            rows = db.execute('SELECT id,tool,side_effect,state,created_at,updated_at FROM work_steps '
                              'WHERE job_id=? ORDER BY created_at,id', (identifier,)).fetchall()
        return [dict(row) for row in rows]

    def record_output(self, step, output):
        with self._db() as db:
            row = db.execute('SELECT state FROM work_steps WHERE id=?', (step,)).fetchone()
            if row is None or row['state'] != 'returned':
                raise JobStateError('Only a successfully returned step can record an inspected output.')
            db.execute('INSERT INTO work_outputs VALUES (?,?,?,?,?)',
                       (step, output['path'], output['sha256'], output['bytes'], output['format']))

    def outputs(self, identifier):
        with self._db() as db:
            rows = db.execute('SELECT o.* FROM work_outputs o JOIN work_steps s ON s.id=o.step_id '
                              'WHERE s.job_id=? ORDER BY s.created_at,s.id', (identifier,)).fetchall()
        return [dict(row) for row in rows]

    def request_cancel(self, identifier):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['state'] in TERMINAL:
                return row['state']
            state = 'cancelling' if row['state'] in ('running', 'cancelling') else 'cancelled'
            db.execute('UPDATE work_jobs SET state=?,note=?,revision=revision+1,updated_at=? WHERE id=?',
                       (state, 'User requested cancellation. Dispatched actions cannot be recalled.', self._now(), identifier))
        return state

    def finish(self, identifier, state, evidence=None):
        if state not in TERMINAL:
            raise ValueError('Invalid terminal state.')
        if state == 'verified' and (not isinstance(evidence, dict) or evidence.get('passed') is not True
                                   or not isinstance(evidence.get('references'), list) or not evidence['references']
                                   or any(not isinstance(x, str) or not x.strip() for x in evidence['references'])):
            raise ValueError('Verified work needs an independent passed check and evidence references.')
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['state'] not in ('running', 'cancelling'):
                raise JobStateError('Job is not running.')
            if row['state'] == 'cancelling' and state != 'cancelled':
                raise JobStateError('Cancellation cannot be overwritten by a late completion.')
            pending = db.execute("SELECT count(*) FROM work_steps WHERE job_id=? AND state IN ('running','uncertain')",
                                 (identifier,)).fetchone()[0]
            if state == 'verified' and pending:
                raise JobStateError('Running or uncertain actions must be inspected before verification.')
            note = json.dumps(evidence, ensure_ascii=False, allow_nan=False) if evidence else ''
            if len(note) > 64000:
                raise ValueError('Evidence exceeds its size limit.')
            db.execute('UPDATE work_jobs SET state=?,note=?,revision=revision+1,updated_at=? WHERE id=?',
                       (state, note, self._now(), identifier))

    def recover_interrupted(self, include_queued=False):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            now = self._now()
            db.execute("UPDATE work_steps SET state='uncertain',updated_at=? WHERE state='running'", (now,))
            states = "('running','cancelling','queued')" if include_queued else "('running','cancelling')"
            count = db.execute("UPDATE work_jobs SET state='paused',note=?,revision=revision+1,updated_at=? "
                               'WHERE state IN ' + states,
                               ('Ano closed during work. Inspect the destination before resuming; no action was replayed.', now)).rowcount
        return count

    def pause(self, identifier):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['state'] not in ('running', 'cancelling'):
                raise JobStateError('Only running work can pause.')
            checkpoint = json.loads(row['checkpoint'])
            checkpoint['requires_reinspection'] = True
            db.execute("UPDATE work_jobs SET state='paused',checkpoint=?,note=?,revision=revision+1,updated_at=? WHERE id=?",
                       (json.dumps(checkpoint), 'Paused. Inspect any dispatched actions before resuming.', self._now(), identifier))

    def resume(self, identifier):
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, identifier)
            if row['state'] != 'paused':
                raise JobStateError('Only a paused job can be resumed.')
            checkpoint = json.loads(row['checkpoint'])
            checkpoint['requires_reinspection'] = True
            db.execute("UPDATE work_jobs SET state='queued',checkpoint=?,revision=revision+1,updated_at=? WHERE id=?",
                       (json.dumps(checkpoint, ensure_ascii=False), self._now(), identifier))

    def resolve_uncertain(self, step, state, evidence):
        if state not in ('verified', 'failed') or not isinstance(evidence, dict):
            raise ValueError('Inspection must resolve the step as verified or failed.')
        if evidence.get('inspected') is not True or not evidence.get('reference'):
            raise ValueError('Independent destination inspection evidence is required.')
        with self._db() as db:
            changed = db.execute("UPDATE work_steps SET state=?,updated_at=? WHERE id=? AND state='uncertain'",
                                 (state, self._now(), step)).rowcount
            if not changed:
                raise JobStateError('Only an uncertain step can be resolved by inspection.')

