# airflow-dbt-postgres-lab

A hands-on **modern data stack** built from scratch with Docker: Apache Airflow 3 orchestrating a dbt project on Postgres. The goal was to understand every moving part — no pre-baked `docker-compose.yml`, no managed service — and to compare two real ways of running dbt from Airflow.

> This is a **learning lab**, not a production deployment. Some choices (SimpleAuthManager, plaintext passwords, LocalExecutor) are deliberately simplified and are called out below with what a production setup would do instead.

---

## What this project demonstrates

- Standing up Airflow 3 as a small cluster (scheduler, API server, DAG processor, metadata DB) in Docker, by hand
- Separating Airflow's metadata database from the analytical warehouse — a boundary that's easy to blur and important to keep clean
- A layered dbt project: **raw → staging → marts**, with materialization controlled per layer, sources, and data-quality tests that actually catch bad data
- **Two orchestration patterns** for the same dbt project, and the trade-off between them:
  - `BashOperator` — simple, keeps dbt isolated, but the whole run is one opaque task
  - **Astronomer Cosmos** — one Airflow task per dbt model and test, at the cost of coupling dbt into Airflow's environment

---

## Architecture

```
                         ┌─────────────────────────────┐
                         │   Docker Compose network     │
                         │                              │
  ┌────────────┐         │   ┌──────────────────────┐   │
  │  Browser   │────────────▶│  airflow-apiserver   │   │
  │ localhost  │  :8080  │   │  (UI + Task API)     │   │
  │  :8080     │         │   └──────────┬───────────┘   │
  └────────────┘         │              │               │
                         │   ┌──────────▼───────────┐   │
                         │   │  airflow-scheduler   │   │
                         │   │  airflow-dag-processor│  │
                         │   └──────────┬───────────┘   │
                         │              │               │
                         │   ┌──────────▼───────────┐   │
                         │   │   airflow-meta        │   │  ← Airflow state only
                         │   │   (Postgres 16)       │   │
                         │   └───────────────────────┘   │
                         │                              │
                         │   ┌───────────────────────┐  │
                         │   │   dbt  (dbt-core 1.9)  │  │
                         │   └──────────┬────────────┘  │
                         │              │ builds models │
                         │   ┌──────────▼────────────┐  │
   DBeaver ─────────────────▶│   warehouse           │  │  ← analytical data
   localhost:55432        │   │   (Postgres 16)       │  │
                         │   │   raw → analytics      │  │
                         │   └───────────────────────┘  │
                         └─────────────────────────────┘
```

**Two Postgres instances on purpose.** `airflow-meta` holds Airflow's own bookkeeping (DAG runs, task states, connections). `warehouse` holds the actual data dbt transforms. Mixing them is a common beginner mistake; keeping them separate makes the boundary explicit — Airflow *orchestrates*, it doesn't *store your data*.

---

## Tech stack

| Component | Version | Role |
|---|---|---|
| Apache Airflow | 3.3.0 | Orchestration |
| Executor | LocalExecutor | Tasks as subprocesses (no Celery/Redis) |
| dbt-core / dbt-postgres | 1.9.x / 1.9.0 | Transformations |
| Astronomer Cosmos | ≥ 1.11 | dbt → Airflow task graph |
| PostgreSQL | 16 | Metadata DB **and** warehouse |
| Python (dbt image) | 3.11 | — |
| Docker Compose | — | Local runtime |

---

## Project structure

```
airflow-dbt-lab/
├── docker-compose.yml         # the whole stack
├── Dockerfile.airflow         # official Airflow image + Docker CLI + dbt/Cosmos
├── Dockerfile.dbt             # standalone dbt image (separate dependency tree)
├── .env                       # AIRFLOW_UID for correct file ownership on Linux
├── dags/
│   ├── hello_lab.py           # smoke-test DAG (TaskFlow API)
│   ├── dbt_pipeline.py        # approach 1: BashOperator → docker exec
│   └── dbt_cosmos.py          # approach 2: Cosmos DbtDag
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml           # git-ignored; commit profiles.yml.example instead
    └── models/
        ├── staging/
        │   ├── sources.yml
        │   ├── stg_customers.sql
        │   ├── stg_orders.sql
        │   └── schema.yml     # data tests
        └── marts/
            └── customer_orders.sql
```

---

## Reproduce it from zero

### Prerequisites
- Docker Desktop (or Docker Engine + Compose v2)
- ~4 GB free for images

### 1. Clone and prepare

```bash
git clone https://github.com/Madraximov/airflow-dbt-postgres-lab.git
cd airflow-dbt-postgres-lab

# On Linux, make mounted volumes writable by the container user (UID 50000).
# Harmless on macOS; set it anyway for portability.
echo "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Build and start

```bash
docker compose build
docker compose up airflow-init      # one-shot: create metadata tables, then exit
docker compose up -d
docker compose ps                   # everything should be Up / healthy
```

### 3. Log in to Airflow

Open <http://localhost:8080>. This lab uses `SIMPLE_AUTH_MANAGER_ALL_ADMINS`, so login is skipped. If you disable that, the generated admin password lives at:

```bash
docker compose exec airflow-apiserver \
  cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### 4. Seed the warehouse

```bash
docker compose exec -T warehouse psql -U dbt -d warehouse < seeds/raw_seed.sql
```

The seed data is **intentionally messy** — a duplicate customer, mixed-case emails and statuses, NULLs, and one order pointing at a non-existent customer — so the staging layer and tests have real work to do.

### 5. Run dbt standalone first

```bash
docker compose exec dbt dbt debug     # expect Connection test: OK
docker compose exec dbt dbt run       # expect PASS=3
docker compose exec dbt dbt test      # expect PASS=4, 1 expected FAIL (see below)
```

### 6. Run it through Airflow

In the UI, trigger `dbt_cosmos` (or `dbt_pipeline`) and open the **Graph** view.

---

## The dbt project

The transformation follows the conventional dbt layering:

**Staging** (`stg_*`, materialized as **views**) cleans the raw data: lower-casing emails and statuses, upper-casing country codes, and de-duplicating customers with a `row_number()` window. Views are cheap and always reflect the current source.

**Marts** (`customer_orders`, materialized as a **table**) joins customers to an order summary and is persisted, because marts get queried often.

Materialization is set **per directory** in `dbt_project.yml`:

```yaml
models:
  lab:
    staging:
      +materialized: view
    marts:
      +materialized: table
```

A model's **folder decides its config** — a `.sql` file placed in the wrong directory silently takes the wrong materialization. (I hit exactly this; see gotchas.)

Dependencies are never wired by hand: `{{ source() }}` and `{{ ref() }}` let dbt build the execution graph itself.

### Data tests

`schema.yml` declares `unique`, `not_null`, and a `relationships` test. The relationships test **fails on purpose**: one seeded order references a customer that doesn't exist, and referential-integrity checking catches it. A failing test here is the test **working**, not the pipeline breaking — it surfaces a real data-quality problem the way it would in production.

---

## Two ways to run dbt from Airflow

### Approach 1 — BashOperator (`dbt_pipeline.py`)

Airflow calls `docker exec` into the standalone dbt container. dbt stays fully isolated in its own image (its own dependency tree, no risk of clashing with Airflow's).

**Trade-off:** the entire `dbt run` is a single Airflow task. If one test out of fifty fails, the whole task goes red and the graph doesn't tell you *which* model broke — you have to read the logs.

### Approach 2 — Astronomer Cosmos (`dbt_cosmos.py`)

Cosmos parses the dbt project and expands **each model and test into its own Airflow task**, deriving dependencies from the dbt graph. In the Graph view you see every model and test as a separate node; the one failing relationships test is a single red node while everything else stays green. This model-level visibility is the industry-standard reason to reach for Cosmos.

**Trade-off:** Cosmos's default `LOCAL` execution mode runs dbt *inside Airflow's Python environment*, so dbt has to be installed into the Airflow image — which reintroduces exactly the dependency coupling that Approach 1 avoids. Cosmos offers isolated modes (`VIRTUALENV`, `DOCKER`, `KUBERNETES`) at extra complexity.

| | BashOperator | Cosmos (LOCAL) |
|---|---|---|
| dbt isolation | ✅ separate image | ❌ shares Airflow env |
| Model-level visibility | ❌ one opaque task | ✅ task per model/test |
| Per-model retries | ❌ | ✅ |
| Setup complexity | Low | Medium |

---

## Design decisions

- **Two separate Postgres containers** so the orchestration/state boundary is explicit.
- **A dedicated dbt image** (`Dockerfile.dbt`) to keep dbt's dependencies from resolving against Airflow's — a well-known way to break the scheduler.
- **LocalExecutor** — no broker to run, ideal for a single-machine lab. Production would use Celery or Kubernetes executors.
- **`profiles.yml` is git-ignored**; a `profiles.yml.example` with placeholders is committed instead. Credentials don't belong in a repo.
- **dbt-core pinned to `<1.10`** — see gotchas.

---

## Gotchas I hit (and how I fixed them)

**`SSL: CERTIFICATE_VERIFY_FAILED` while building the dbt image.**
`dbt-postgres==1.9.0` left `dbt-core` unpinned, so pip pulled dbt-core 1.12, which downloads a wheel from GitHub at build time. On a network that does TLS inspection (self-signed cert in the chain), that download fails while PyPI works fine. **Fix:** pin `dbt-core>=1.9,<1.10`, which installs entirely from PyPI. The same interception will affect `dbt deps` and any git-based install.

**`relation "..._dbt_backup" already exists`.**
dbt rebuilds a relation by creating a new object under a `_dbt_backup` name and renaming over the old one. An interrupted run leaves that backup object behind, and the next run refuses to recreate it. **Fix:** `DROP SCHEMA analytics CASCADE;` and re-run — everything in `analytics` is dbt-generated and reproducible. In production you'd drop just the stray `_dbt_backup` object.

**Mart built as a view, not a table.**
The `marts` config didn't apply and dbt warned about an "unused configuration path." The model file was in the wrong folder — **a model's directory determines its materialization.** Moving it into `models/marts/` fixed it.

**Airflow login wall.**
Airflow 3 config env vars follow `AIRFLOW__{SECTION}__{KEY}`, and Airflow **silently ignores unrecognized vars**. A mis-sectioned auth setting had no effect. `airflow config get-value <section> <key>` shows what Airflow actually resolved — the go-to debugging command.

---

## What I learned

- How Airflow 3 is actually assembled from separate processes, and why the Task Execution API exists (tasks call back into the API server instead of importing Airflow's models).
- The dbt layering discipline — and that folder placement, not just SQL, drives behavior.
- That "pinned" dependencies aren't pinned if their transitive deps are open, which is why real dbt projects commit a lockfile.
- The concrete trade-off between a simple, isolated orchestration wrapper and a model-aware one.

## Possible next steps

- Filter the orphaned order in staging to make the whole DAG green (full "test caught it → fixed it" cycle)
- Incremental models and `depends_on_past`
- A proper auth manager (FabAuthManager / Keycloak) and secrets backend
- Cosmos in `VIRTUALENV` mode to restore dbt isolation

---

*Built as a self-directed lab to deepen my orchestration + transformation fundamentals.*