# Deploying This Project to a VM — From Scratch

A step-by-step procedure for deploying this repo to a fresh Linux VM,
written so an LLM (or a human) with no prior context on this project can
follow it end to end. It assumes root SSH access to a brand-new VM with
nothing installed yet, and reproduces exactly what was done to deploy
this project the first time — including the fixes for bugs that were
found and fixed along the way, which are already baked into this repo's
current state (see "Bugs already fixed" at the bottom — you will not
hit these again if deploying from this repo as it stands).

For the specific, currently-deployed VM's IP/credentials/live status,
see `docs/VM_ACCESS.md` (gitignored, not in this file — that one has
real secrets, this one doesn't).

## What you're deploying

A Dockerized data pipeline: a Python event producer → Kafka → a Python
ingestion consumer → PostgreSQL → dbt (transforms) → Grafana (dashboards)
+ Prometheus (metrics). 9 containers total, defined in one
`docker-compose.yml`. 6 of them pull public images from Docker Hub; 3
(`producer`, `ingestion`, `retry`) are built locally from this repo's
own Dockerfiles on the VM.

## Prerequisites

- A fresh Linux VM (this was done on Ubuntu 26.04 LTS) with a public IP,
  root SSH access, at least ~2 vCPUs / 4GB RAM / 20GB disk.
- An SSH keypair on your local machine you want to use for ongoing
  access (generate one with `ssh-keygen -t ed25519` if you don't have one).
- This repo, locally, on the machine you're deploying from.

## Step 1 — Initial connection and prerequisite packages

```bash
ssh root@<VM_IP>   # password auth, first time only
```

Install Docker and basic tooling:

```bash
apt-get update -qq
apt-get install -y -qq ca-certificates curl gnupg ufw fail2ban

curl -fsSL https://get.docker.com | sh
systemctl enable --now docker
docker --version
docker compose version
```

## Step 2 — Create a non-root sudo user with key-based access

Don't deploy as root long-term. Create a dedicated user (this project
used `deploy`):

```bash
useradd -m -s /bin/bash -G sudo,docker deploy
mkdir -p /home/deploy/.ssh
echo '<YOUR_PUBLIC_KEY_CONTENTS>' > /home/deploy/.ssh/authorized_keys
chmod 700 /home/deploy/.ssh
chmod 600 /home/deploy/.ssh/authorized_keys
chown -R deploy:deploy /home/deploy/.ssh
echo 'deploy ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/deploy
chmod 440 /etc/sudoers.d/deploy
passwd -l deploy   # lock password login for this user - key only
```

**Verify it works before touching SSH hardening** — if you lock
yourself out of root before confirming key access, you're stuck:

```bash
ssh -i <your_private_key> deploy@<VM_IP> "whoami && sudo whoami && docker ps"
```

## Step 3 — SSH hardening (optional, recommended for anything public-facing)

```bash
sudo tee /etc/ssh/sshd_config.d/99-hardening.conf >/dev/null <<'EOF'
PasswordAuthentication no
PermitRootLogin no
KbdInteractiveAuthentication no
EOF
sudo sshd -t && sudo systemctl reload ssh
```

Confirm root/password login is actually blocked and key login still
works before moving on. (This step is a judgment call — if you want
root password access preserved for convenience, skip it or use
`PermitRootLogin yes` / `PasswordAuthentication yes` instead. Either
way, whatever password you leave in place should be assumed
compromised the moment it's typed into a chat/terminal log, and
rotated if that matters to you.)

## Step 4 — Firewall

Only expose what actually needs to be public. This stack only needs
SSH and Grafana reachable from outside:

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow OpenSSH
sudo ufw allow 3000/tcp comment 'Grafana'
sudo ufw --force enable
sudo systemctl enable --now fail2ban
```

Everything else (Kafka, Postgres, Prometheus, Alertmanager,
kafka-exporter, node-exporter) is bound to `127.0.0.1` in
`docker-compose.yml` (see Step 6), so ufw is a second layer of defense,
not the only one.

## Step 5 — Get the code onto the VM

**Option B — git clone directly on the VM (current setup; needed for
CI-driven deploys)**. This repo is public, so no deploy key/PAT is
needed to pull:

```bash
git clone https://github.com/zain2983/kafka-ecommerce-pipeline.git /home/deploy/DE-Demo-Project
```

Ongoing deploys are then just `git fetch && git reset --hard
origin/main` in that directory, followed by `docker compose up -d
--build` — see `scripts/deploy_remote.sh` and "CI-driven deploys"
below, which run exactly that, triggered from GitHub Actions.

**Option A — rsync from your local working copy** (how this project
was *first* deployed, before switching to Option B — kept here since
it's still the simplest path if you'd rather not give the VM any tie
to GitHub, e.g. for a private repo with no deploy key set up):

```bash
rsync -avz -e "ssh -i <your_private_key>" \
  --exclude='.git' --exclude='.venv' --exclude='__pycache__' --exclude='*.pyc' \
  --exclude='dbt/target' --exclude='dbt/logs' \
  /path/to/local/DE-Demo-Project/ \
  deploy@<VM_IP>:/home/deploy/DE-Demo-Project/
```

**Caution:** when rsyncing multiple individual files to one destination
directory (rather than a whole directory tree), rsync flattens their
paths — `rsync a/b/file.yml some_dir/` does NOT recreate `a/b/`, it
drops `file.yml` directly in `some_dir/`. Always sync whole directories,
or use `--relative`, or do one file at a time with its full destination
path spelled out. An rsync'd directory has no `.git`, so it can't be
`git fetch`/`reset` — don't mix the two options on the same directory.

## Step 6 — Configure environment for this VM

`docker-compose.yml` reads secrets/config from a `.env` file in the
repo root (gitignored, created fresh per-deployment — never committed).
Generate real random passwords, don't reuse the local-dev defaults:

```bash
cd /home/deploy/DE-Demo-Project
cat > .env <<EOF
POSTGRES_PASSWORD=$(openssl rand -base64 18 | tr -dc 'a-zA-Z0-9' | head -c24)
GF_SECURITY_ADMIN_PASSWORD=$(openssl rand -base64 18 | tr -dc 'a-zA-Z0-9' | head -c24)
KAFKA_ADVERTISED_HOST=<VM_IP>
KAFKA_BIND=127.0.0.1
POSTGRES_BIND=127.0.0.1
PROMETHEUS_BIND=127.0.0.1
KAFKA_EXPORTER_BIND=127.0.0.1
ALERTMANAGER_BIND=127.0.0.1
EOF
chmod 600 .env
```

What each variable does:
- `KAFKA_ADVERTISED_HOST` — **critical for remote deploys.** Kafka tells
  clients where to reconnect via its "advertised listener." Left at the
  code's default (`localhost`), any client connecting from outside the
  VM itself gets told to reconnect to *its own* machine, which fails.
  Must be the VM's real IP (or hostname) here.
- `*_BIND` variables — which network interface each port binds to.
  `127.0.0.1` means "only reachable from inside the VM," which is what
  you want for everything except Grafana (which isn't in this list —
  it's intentionally left on `0.0.0.0:3000` in `docker-compose.yml` so
  it's reachable from outside, matching the ufw rule in Step 4).
- `POSTGRES_PASSWORD` / `GF_SECURITY_ADMIN_PASSWORD` — self-explanatory;
  read by both the `postgres`/`grafana` containers and by the
  `ingestion`/`retry` containers and the Grafana Postgres datasource
  provisioning file (`grafana/provisioning/datasources/datasources.yml`,
  which expands `$POSTGRES_PASSWORD` from the `grafana` container's env).

## Alerting (`prometheus/alerts.yml`, `prometheus/alertmanager.yml`)

design.md section 29.2. Prometheus evaluates three rules against the
same metrics the Grafana dashboard already charts — consumer lag stuck
high for 10+ minutes, the DLQ growing at a sustained rate, or a scrape
target (kafka-exporter/node-exporter) down for 2+ minutes — and fires
them to the `alertmanager` container, which is what actually notifies
someone.

`prometheus/alertmanager.yml` ships with its webhook receiver pointed
at a placeholder URL (`http://localhost:5001/replace-with-a-real-webhook`)
committed as real config, the same way `prometheus.yml` and the Grafana
dashboard JSON are — not templated via `.env`, since Alertmanager has no
built-in env-var expansion for its config file. `docker compose up`
still starts cleanly with the placeholder in place; alerts fire and
Alertmanager tries to deliver them, they just go nowhere (visible in
`docker compose logs alertmanager`) until this is pointed at something
real:

- **Generic webhook** (default receiver, `ops-webhook`): edit the `url`
  under `receivers:` in `prometheus/alertmanager.yml` to point at
  anything that accepts Alertmanager's webhook payload — a low-code
  automation tool (e.g. an n8n/Zapier webhook trigger), or your own
  small receiver.
- **Slack**: uncomment the `ops-slack` receiver block in the same file,
  fill in a real Slack incoming-webhook URL, and change `route.receiver`
  from `ops-webhook` to `ops-slack`.

After editing, redeploy (`docker compose up -d alertmanager` restarts
just that container and picks up the new config) or push to `main` and
let the CI-driven deploy pick it up on the next run.

To confirm alerts actually reach Alertmanager, briefly kill a container
an alert covers (e.g. `docker kill ecommerce-kafka-exporter`, then
`docker compose up -d kafka-exporter` to bring it back once you've
confirmed it) and check `http://<VM_IP or localhost>:9093/api/v2/alerts`
after the rule's `for:` window elapses — this is exactly how the
alerting setup itself was verified during development.

## Step 7 — Bring the stack up

```bash
cd /home/deploy/DE-Demo-Project
docker compose up -d --build
```

This is the step that does the image pulling / building described at
the top: Docker pulls `apache/kafka:4.0.0`, `postgres:16-alpine`,
`danielqsj/kafka-exporter:latest`, `prom/node-exporter:latest`,
`prom/prometheus:latest`, `grafana/grafana-oss:latest` from Docker Hub
(first run only — cached after that), and builds the `producer` and
`ingestion` images locally from their Dockerfiles (`retry` reuses the
`ingestion` image with a different command).

Wait for `kafka` and `postgres` to report `healthy`
(`docker ps` — both have healthchecks), then create the Kafka topics:

```bash
docker cp kafka/init/create_topics.sh ecommerce-kafka:/tmp/create_topics.sh
docker exec -e KAFKA_BOOTSTRAP_SERVER=localhost:29092 ecommerce-kafka bash /tmp/create_topics.sh
```

(Note: `localhost:29092`, the internal listener, not `9092` — running
the topic-creation command against the external/advertised listener
from inside the same container that owns it can hit connection issues.
See "Bugs already fixed" #3.)

## Step 8 — Python environment + dbt

```bash
sudo apt-get install -y -qq python3-venv python3-pip
python3 -m venv .venv
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r producer/requirements.txt -r ingestion/requirements.txt -r dbt/requirements.txt
```

Run dbt once to build the analytics tables:

```bash
set -a; source .env; set +a
export POSTGRES_HOST=localhost   # dbt runs on the host, needs the published port, not the docker service name
cd dbt && DBT_PROFILES_DIR=. dbt run
```

Then set up the recurring refresh (this project schedules it every 15
minutes, matching `analytics.sales_by_interval`'s own bucket size):

```bash
cd /home/deploy/DE-Demo-Project
mkdir -p logs
CRON_LINE="*/15 * * * * /home/deploy/DE-Demo-Project/scripts/run_dbt.sh >> /home/deploy/DE-Demo-Project/logs/dbt_cron.log 2>&1"
( crontab -l 2>/dev/null | grep -v "run_dbt.sh" ; echo "$CRON_LINE" ) | crontab -
```

## Step 9 — Backups

design.md section 29.3: `postgres_data` was a single Docker volume with
no dump/snapshot process at all until this step. Set up the recurring
Postgres backup the same way as the dbt refresh above — cron running
`scripts/backup_postgres.sh` (real `pg_dump`, custom format, into
`backups/postgres/`, with old dumps pruned automatically — see that
script's own header for the restore procedure):

```bash
cd /home/deploy/DE-Demo-Project
mkdir -p backups/postgres
CRON_LINE="0 */6 * * * /home/deploy/DE-Demo-Project/scripts/backup_postgres.sh >> /home/deploy/DE-Demo-Project/logs/postgres_backup_cron.log 2>&1"
( crontab -l 2>/dev/null | grep -v "backup_postgres.sh" ; echo "$CRON_LINE" ) | crontab -
```

Every 6 hours, retaining 7 days (`BACKUP_RETENTION_DAYS` in `.env` to
change), is the accepted RPO/storage tradeoff for this project — see
`docs/FAILURE_SCENARIOS.md`'s "Data loss / backup story" section for
the full reasoning, including the equivalent decision made for Kafka
(no backup mechanism, a documented accepted-loss window instead).

Confirm it actually works before moving on, the same drill
`tests/postgres/test_backup_restore.py` automates in CI:

```bash
./scripts/backup_postgres.sh
ls -la backups/postgres/
```

## Step 10 — Verify end to end

```bash
python3 -c "
import sys, os
sys.path.insert(0, 'tests/grafana')
import verify_stack
verify_stack.GRAFANA_AUTH = ('admin', os.environ['GF_SECURITY_ADMIN_PASSWORD'])
verify_stack.main()
"
```

This checks Grafana health, both datasources, the dashboard is
provisioned, every PromQL query in it executes, and every SQL query in
it executes both directly and through Grafana's proxy. Should end with
`All checks passed.`

Then visit `http://<VM_IP>:3000`, log in with the Grafana credentials
from `.env`, and confirm the dashboard is showing live data.

## CI-driven deploys (`.github/workflows/deploy.yml`)

Once the VM is on Option B (a real `git clone`), a push to `main`
reaches the VM automatically: `.github/workflows/deploy.yml` triggers
via `workflow_run` once `ci.yml` reports success on `main` (it will
NOT deploy a red build — see the `if:` on the `deploy` job). It SSHes
into the VM with a **dedicated, restricted** key and runs
`scripts/deploy_remote.sh` there, which does what Step 7 above does by
hand: `git fetch && git reset --hard origin/main`, bring up
kafka/postgres/monitoring, wait for health, re-create Kafka topics,
then rebuild/restart producer/ingestion/retry (see that script's own
comment for why infra-and-topics comes before the app services on this
VM specifically). `workflow_dispatch` still works too, for a manual
one-off re-run.

### One-time setup for this (already done for the current VM)

1. Generate a dedicated keypair — don't reuse a personal key:
   ```bash
   ssh-keygen -t ed25519 -f gha_deploy_key -N "" -C "github-actions-deploy@<repo>"
   ```
2. Install the **public** half on the VM, restricted so this key can
   run nothing but the deploy script, regardless of what command a
   client sends — this is what caps the damage if the private half
   (held only as a GitHub Actions secret) ever leaks:
   ```
   command="/home/deploy/DE-Demo-Project/scripts/deploy_remote.sh",no-port-forwarding,no-X11-forwarding,no-agent-forwarding ssh-ed25519 AAAA... github-actions-deploy@<repo>
   ```
   appended as its own line in `~deploy/.ssh/authorized_keys`.
3. Store the **private** half and connection details as repo secrets
   (Settings → Secrets and variables → Actions), then delete any local
   copy of the private key:
   - `DEPLOY_SSH_KEY` — the private key file's full contents
   - `DEPLOY_HOST` — the VM's IP
   - `DEPLOY_USER` — `deploy`

From then on, "deploy" is just: push to `main` (or merge a PR). CI runs,
and if it's green, `Deploy to VM` fires on its own. The VM directory is
a deploy target now, not somewhere to hand-edit — `deploy_remote.sh`'s
`git reset --hard` will discard any local changes made directly on the
box.

## Bugs already fixed in this repo (informational — you won't hit these deploying from current `main`)

These were found and fixed during the first real deployment of this
project. They're listed here so nobody re-discovers them from scratch —
the fixes are already in `docker-compose.yml` / the dbt models / the
Grafana dashboard as of this writing:

1. **Kafka lost all data on every container recreate** — the compose
   file mounted a volume at `/var/lib/kafka/data` but Kafka's actual
   `log.dirs` defaulted to `/tmp/kafka-logs` (ephemeral), so the mount
   did nothing. Fixed with `KAFKA_LOG_DIRS: /var/lib/kafka/data`.
2. **Services didn't survive a VM reboot** — `restart: on-failure`
   doesn't bring containers back after a full Docker daemon restart.
   Fixed by using `restart: unless-stopped` everywhere.
3. **Creating topics against the wrong listener from inside the Kafka
   container itself** can hit a hairpin-NAT-style connection issue if
   the advertised listener is the VM's public IP — use the internal
   listener (`localhost:29092`) for anything running on the Kafka
   container itself.
4. **`node-exporter` unreachable from Prometheus** when configured with
   `network_mode: host` — Docker's inter-network bridge isolation
   blocks it from being reached by container name from a different
   compose network. Fixed by keeping node-exporter on the normal
   compose network with `/proc`, `/sys`, `/` bind-mounted instead
   (sufficient for CPU/mem/disk metrics; host networking is only needed
   for per-interface network throughput, unused here).
5. **Dashboard panels hardcoded time windows in SQL** instead of using
   Grafana's `$__timeFilter()` macro, so the date-range picker did
   nothing for most panels. Fixed across all business-metric panels.

## Common mistakes to avoid

- Don't skip setting `KAFKA_ADVERTISED_HOST` — this is the single most
  common way a "works locally, breaks on a VM" failure happens with
  this project.
- Don't bind Kafka/Postgres/Prometheus to `0.0.0.0` on a public VM —
  Kafka in particular has zero authentication in this setup.
- Don't assume `docker compose down && up` preserves state unless
  you've confirmed the relevant volume mounts actually work (see bug #1
  above) — verify with a real restart test before trusting it.
- Check `docker ps -a` (not just `docker ps`) when debugging — a
  container that's `Exited` won't show in the plain form and is easy to
  miss.
