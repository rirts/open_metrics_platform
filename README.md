# Open Metrics Platform (dbt + Postgres)

Analytics project for the Olist dataset built with **dbt** (Postgres), including
layered models (staging → intermediate → marts), tests, linting, and a Metabase
executive dashboard exposure.

> **Note on licensing**
> This repository is **All rights reserved**. No license is granted for use,
> copying, modification, or distribution.

---

## Overview

- **Warehouse**: Postgres
- **Orchestration**: dbt `>=1.10,<1.11`
- **Linting**: SQLFluff (dbt templater)
- **Dashboard**: Metabase (exposure wired via env var)

The project models daily and monthly sales KPIs, month-over-month and YoY
metrics (with a 3-month moving average and caps), and applies lightweight
performance improvements (idempotent indexes) on raw tables and select marts.

---

## Project Structure
```text
dbt_project/
├─ models/
│  ├─ staging/olist/           # source normalization (views)
│  ├─ intermediate/olist/      # business-friendly base (views)
│  └─ marts/olist/             # consumable KPIs (tables/views)
│     └─ schema.yml            # tests & contracts
├─ seeds/                      # CSV seeds (olist datasets & params)
├─ macros/                     # utility macros (e.g., create_indexes.sql)
├─ profiles/                   # local profiles (optional; typically ~/.dbt)
└─ dbt_project.yml

```


---

## Key Models

### Staging
- `stg_olist__*` (views): direct source cleanup & typing.

### Intermediate
- `int_olist__orders` (view): normalized order fields & dates.
- `int_olist__order_items` (view): item totals + light enrichments.

### Marts
- `mart_olist__sales_daily` (table): daily `orders`, `items`, `revenue`, `freight`, `aov`.
  Post-hook creates `idx_mart_olist__sales_daily_order_date` on `(order_date)`.
- `mart_olist__sales_monthly` (table): monthly rollup of the daily mart.
  Post-hook creates `idx_mart_olist__sales_monthly_month` on `(month)`.
- `mart_olist__monthly_growth` (view): 3-month MA, MoM %, YoY(MA3) %.
- `mart_olist__monthly_kpis` (view, contracted schema): monthly KPIs + MA(3).
- `mart_olist__yoy_anomalies` (view, contracted schema): YoY(MA3) with floor/cap.

---

## Seeds

- `olist_*_dataset.csv` families (customers, orders, order_items, payments, products, sellers, reviews, geolocation)
- `params_yoy.csv` (single row: `yoy_floor`, `yoy_cap`, `min_base`)
- `product_category_name_translation.csv`
- `kpi_dictionary.csv`

---

## Metabase Exposure

- `exposures:` in `marts/olist/schema.yml` references an executive dashboard.
  Configure the URL via the `METABASE_URL` env var (defaults to `http://localhost:3000`).

---

## Getting Started

### 1) Environment
- Python 3.10+ recommended
- Postgres access (host, db, user with DDL privileges on target schemas)
- dbt-core `>=1.10,<1.11` and dbt-postgres plugin

```bash
# (optional) create & activate venv
python -m venv .venv
# Windows PowerShell:
. .\.venv\Scripts\Activate.ps1
# macOS/Linux:
# source .venv/bin/activate

pip install -r requirements.txt  # if present
# or:
pip install dbt-postgres==1.10.* sqlfluff==3.* sqlfluff-templater-dbt==3.*
```

### 2) Configure your dbt profile

#### Option A (repo-local, used by the commands below)
Create `.\profiles\profiles.yml` in the repo and keep `--profiles-dir .\profiles` in your dbt commands.

```yaml
open_metrics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: 127.0.0.1
      port: 5432
      user: analytics
      password: analytics_pwd
      dbname: analytics
      schema: analytics
      threads: 4
      sslmode: disable
```

#### Option B (global, optional)
Put the same YAML in `%USERPROFILE%\.dbt\profiles.yml` (Windows) or `~/.dbt/profiles.yml` (macOS/Linux) and omit `--profiles-dir` in commands.


### 3) Dependencies & Seeds

Install packages and load the CSV seeds into your warehouse.

```bash
dbt deps --project-dir .\dbt_project

dbt seed --project-dir .\dbt_project --profiles-dir .\profiles --target dev
```

(Optional) Rerun seeds from scratch:

```bash
dbt seed --project-dir .\dbt_project --profiles-dir .\profiles --target dev --full-refresh
```

### 4) Build Everything

Compile and run all models, then execute tests defined in `schema.yml`.

```bash
dbt build --project-dir .\dbt_project --profiles-dir .\profiles --target dev
```

#### Run specific marts only

```bash
dbt run --project-dir .\dbt_project --profiles-dir .\profiles --target dev --select mart_olist__sales_daily mart_olist__sales_monthly
```

(Optional) Force a full rebuild of those marts:

```bash
dbt run --project-dir .\dbt_project --profiles-dir .\profiles --target dev --select mart_olist__sales_daily mart_olist__sales_monthly --full-refresh
```

## Linting & Style

We use SQLFluff (dbt templater). Typical workflow:

### Lint a subset

```bash
sqlfluff lint dbt_project --config .\.sqlfluff
```

### Auto-fix layout/spacing where safe

```bash
sqlfluff fix dbt_project --config .\.sqlfluff
```

## Performance

Idempotent index creation (optional but recommended).

### Create helper indexes on raw seeds (and optionally marts)

```bash
dbt run-operation create_indexes --project-dir .\dbt_project --profiles-dir .\profiles --target dev --args '{schema: "analytics_analytics_raw", also_marts: true}'
```

- **Raw tables**: indexes on common join/filter columns (e.g., `order_id`, `customer_id`).
- **Marts**:
  - `mart_olist__sales_daily` → index on `(order_date)`
  - `mart_olist__sales_monthly` → index on `(month)`

**Notes**
- Safe to rerun (`IF NOT EXISTS`).
- Does **not** change Metabase results; it only improves query performance.


## Testing & Data Quality

- **Schema tests** live in `dbt_project/models/marts/olist/schema.yml`:
  - `not_null`, `unique`, `accepted_range`, `expression_is_true`.
- **Contracts** are enforced on select marts (column names & types).
- **Reconciliation checks**: custom `monthly_equals_daily` tests ensure monthly totals match daily aggregates within a tolerance.

### Run all tests
```bash
dbt test --project-dir .\dbt_project --profiles-dir .\profiles --target dev
```

### Helpful subsets
```bash
# Only tests for one model
dbt test --project-dir .\dbt_project --profiles-dir .\profiles --target dev -s mart_olist__monthly_kpis

# Only schema tests (skip any singular data tests)
dbt test --project-dir .\dbt_project --profiles-dir .\profiles --target dev --schema
```

**Notes**
- If you see “relation does not exist”, run a full build first:
  ```bash
  dbt build --project-dir .\dbt_project --profiles-dir .\profiles --target dev
  ```


## Troubleshooting

### “No dbt_project.yml found at expected path”
- Run commands from the repo **root** and point dbt at the project dir:
  ```bash
  dbt build --project-dir .\dbt_project --profiles-dir .\profiles --target dev
  ```
- For cleaning compiled state:
  ```bash
  # from inside the project folder
  cd .\dbt_project
  dbt clean
  # if dbt refuses to clean outside the project, remove the folder manually:
  Remove-Item .\target -Recurse -Force   # PowerShell (Windows)
  ```

### “relation … does not exist”
- The model/view hasn’t been created yet or was dropped. Do a full build:
  ```bash
  dbt build --project-dir .\dbt_project --profiles-dir .\profiles --target dev
  ```
- If you changed schema names or materializations, force a rebuild:
  ```bash
  dbt run --project-dir .\dbt_project --profiles-dir .\profiles --target dev --full-refresh
  ```

### Seeds are very slow (large CSVs)
- First-time seeding loads ~1M rows; that’s expected.
- After seeding, run helper indexes to speed downstream queries:
  ```bash
  dbt run-operation create_indexes --project-dir .\dbt_project --profiles-dir .\profiles --target dev --args '{schema: "analytics_analytics_raw", also_marts: true}'
  ```

### Post-hook / index errors (e.g., “improper qualified name”)
- In **model** post-hooks, only index **columns of that model**:
  ```sql
  {{ config(post_hook=['create index if not exists idx_mart_olist__sales_daily_order_date on {{ this }} (order_date)']) }}
  ```
- For indexing **other tables** (e.g., raw seeds), use `dbt run-operation create_indexes` instead of a model post-hook.

### Lint errors (SQLFluff)
- **Trailing semicolons**: Remove `;` at the end of model SQL files.
- **USING vs ON** (ST07): Prefer explicit `JOIN … ON a.col = b.col` over `USING(...)`.
- Auto-fix layout/spacing where safe:
  ```bash
  sqlfluff fix dbt_project --config .\.sqlfluff
  ```

### Partial parsing warnings
- Harmless when config/profile changes. If it persists:
  ```bash
  cd .\dbt_project
  dbt clean
  ```

### Profile/schema mismatches
- Ensure the **profile name** in `dbt_project.yml` matches the entry in your profiles file (`open_metrics`).
- Verify schemas:
  - `models` default schema → `analytics`
  - `seeds` schema override → `analytics_raw`
- If you moved from global to repo-local profiles (or vice versa), either:
  - Add `--profiles-dir .\profiles` to commands, **or**
  - Place the same YAML under `%USERPROFILE%\.dbt\profiles.yml`.

### Windows quoting / line continuation
- Use single quotes around `--args` payloads:
  ```bash
  --args '{schema: "analytics_analytics_raw", also_marts: true}'
  ```
- PowerShell line continuation uses backtick:
  ```powershell
  dbt run `
    --project-dir .\dbt_project `
    --profiles-dir .\profiles `
    --target dev
  ```

### Permissions
- The database user must have `CREATE` on target schemas (`analytics`, `analytics_raw`) and rights to `CREATE INDEX`.


## Conventions

- **Naming**
  - Models follow prefixes: `stg_…`, `int_…`, `mart_…`
  - Columns in `snake_case`; avoid abbreviations unless obvious (`id`, `ts`)
- **SQL style**
  - Keywords consistently **lowercase**; single space around `AS`
  - **No trailing semicolons** in model files
  - Use explicit joins: `JOIN … ON a.col = b.col` (avoid `USING`)
  - Prefer explicit column lists in marts; `SELECT *` allowed only in staging/intermediate when intentional
- **CTEs & aliases**
  - Small, meaningful CTE names; concise aliases (`o`, `oi`) when context is clear
  - Qualify join keys and ambiguous columns
- **Types & casting**
  - Cast deterministically (`::date`, `::numeric(18,2)`) and avoid implicit casts in joins/filters
- **dbt specifics**
  - Always use `{{ ref() }}` and `{{ source() }}` (no hard-coded schemas)
  - Tests & contracts live in `schema.yml`; keep column order stable when using contracts
  - Use `materialized='table'` only for marts that benefit; prefer views elsewhere
  - Keep post-hooks (indexes) **idempotent** and scoped to the model with `{{ this }}`
- **Linting**
  - Run `sqlfluff lint` and `sqlfluff fix` before committing; resolve CP/LT/ST/RF/AL findings
- **Versioning & CI**
  - Do not commit `target/`; run `dbt build` in CI
- **Documentation**
  - Add model/column descriptions in `schema.yml`; keep `exposures` current


## License

This repository is **All rights reserved**.

No permission is granted to use, copy, modify, or distribute this work without
explicit written consent from the copyright holder.
