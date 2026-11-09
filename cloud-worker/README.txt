Ano headless worker foundation — not deployed

Implemented and tested: scoped text_report jobs, SQLite queue, single-worker
lock, exact UTF-8 read-back, receipts, cancellation and interrupted-job holds.
Gateway adds single-owner bearer authentication, atomic idempotency keys,
authenticated status/cancellation/output retrieval, a 64KB request limit and
10,000-job retention cap. Client requires API route and HTTPS, rejects redirects,
checks downloaded output hashes and refuses to overwrite local files.

The original 18 tests pass on macOS. All 21 tests pass inside Linux, including
Gunicorn HTTP execution, graceful restart, worker-death supervision and missing-token rejection. A real loopback HTTP test exercises
submit → worker execution → verified download. Production image is non-root.
Local Docker volume checks confirm persistence and no replay after replacement.
No cloud service, native UI connection or AI reasoning is claimed.

Run from this directory:
  python3 -m pip install gunicorn==23.0.0
  python3 -m unittest discover -v
  python3 verify_container.py
  python3 verify_volume.py

CLI submission remains available only to trusted local operators:
  python3 -m ano_worker --data-dir /private/tmp/ano-worker-state submit request.json
  python3 -m ano_worker --data-dir /private/tmp/ano-worker-state once
  python3 -m ano_worker --data-dir /private/tmp/ano-worker-state status
request.json:
  {"route":"api","kind":"text_report","text":"Exact report body."}

WSGI interface:
  from ano_worker.gateway import Gateway
  application = Gateway(data_directory, private_token)
Never use the test-only wsgiref server for public deployment.
POST /v1/jobs requires Authorization: Bearer TOKEN and Idempotency-Key.
GET /v1/jobs/JOBID returns state and output metadata.
GET /v1/jobs/JOBID/output returns only verified, hash-matching text.
POST /v1/jobs/JOBID/cancel requests cancellation.
Use a randomly generated private token; do not put it in Git, logs or chat.
One token represents one owner; this is not a multi-user authorization system.
The route field alone is not authorization. The authenticated caller and client
route guard must be integrated with Ano's actual active-route policy.

Remaining before deployment:
- Native Ano settings/Keychain/review UI and hosted flow verification (code wired; 300 native regression tests pass).
- Reviewed source published to the authorized deployment branch.
- Render Web Service configuration, persistent disk ownership and TLS checks.
- Approval of exact recurring costs and public endpoint creation.

The Docker CMD starts ano_worker.serve, which supervises one queue worker and
one Gunicorn HTTP master sharing ANO_DATA_DIR (default /var/data/ano).
Set ANO_GATEWAY_TOKEN privately (at least 32 ASCII characters) and PORT (default
10000). HTTP binds 0.0.0.0; deploy behind the hosting provider's TLS proxy.
All HTTP paths require authentication; configure a TCP startup check.
If either child exits, the supervisor stops the other and exits nonzero for
host restart. SIGTERM permits up to 15 seconds for graceful shutdown. Do not scale replicas against this local store.
No broker orders, arbitrary commands, desktop control or external messages exist.
