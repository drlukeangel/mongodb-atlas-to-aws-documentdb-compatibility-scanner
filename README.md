# MongoDB Atlas to Amazon DocumentDB Compatibility Scanner

Automated compatibility assessment tool that scans MongoDB Atlas clusters and generates a detailed migration readiness report for Amazon DocumentDB.

## What It Does

Connects to your Atlas clusters via the Admin API + a temporary read-only database user, runs four scanning passes, then produces a single consolidated report:

1. **Operator compatibility** (live URI) -- identifies unsupported query/aggregation operators via the cluster profiler
2. **Operator compatibility** (log scan) -- downloads Atlas server logs and scans for unsupported operators executed over the past N days
3. **Index compatibility** -- dumps all indexes and flags those incompatible with DocumentDB (text, wildcard, partial filters, etc.)
4. **Feature compatibility** (live scan) -- inspects collection metadata for unsupported features: time-series, GridFS, capped collections, clustered indexes, CSFLE, change-stream pre/post images, schema validators, views with unsupported stages, server-side JS, and sharded collections

The final report includes:

- Migration readiness breakdown (ready / code-changes-needed / AWS-service-swap)
- Unsupported operator inventory with specific DocumentDB migration paths
- Per-project, per-cluster, per-database remediation plan
- Effort estimates in developer-hours (and optional dollar cost)

## Prerequisites

- Python 3.9+
- An Atlas programmatic API key with **Project Read Only** permissions (minimum)
- Network access from your machine to Atlas clusters (for the live scans)

For multi-project scanning, you also need an Atlas **Organization Service Account** with Project Creator + Read Only permissions.

## Quick Start

```bash
# 1. Clone this repo
git clone <repo-url> mongo-docdb-compat
cd mongo-docdb-compat

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure credentials
cp .env.example .env
# Edit .env with your Atlas API keys and project ID

# 4. Run the scan
python run_compat_check.py
```

Reports are written to `results/`. The main output is `results/project_summary.txt`.

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `atlas_public_key` | Yes | Atlas API public key |
| `atlas_private_key` | Yes | Atlas API private key |
| `atlas_group_id` | Yes | Atlas project (group) ID |
| `atlas_cluster_name` | No | Scan only this cluster (omit to scan all) |
| `atlas_organization_Client_ID` | No | Service account client ID (enables multi-project mode) |
| `atlas_organization_Client_Secret` | No | Service account client secret |
| `ATLAS_LOG_DAYS` | No | Days of logs to download (default: 7) |
| `ENGINEER_HOURLY_RATE` | No | Hourly rate for cost estimates (default: 0 = omit) |

## Multi-Project Scanning

If you set `atlas_organization_Client_ID` and `atlas_organization_Client_Secret`, the scanner will discover all projects visible to the service account and scan every cluster across the organization.

## Test Harness

`setup_test_env.py` creates a realistic multi-project Atlas environment with clusters seeded with every DocumentDB-incompatible feature. Use it to validate the scanner end-to-end:

```bash
# Full test: create clusters, seed data, scan, teardown
python setup_test_env.py

# Keep clusters alive for manual inspection
python setup_test_env.py --no-teardown

# Clean up leftover test resources
python setup_test_env.py --teardown-only
```

This requires the organization service account credentials in `.env`.

## Project Structure

```
atlas_api.py          # Atlas API client classes (Digest + OAuth2 auth)
run_compat_check.py   # Main scanner and report generator
setup_test_env.py     # Test harness (creates/seeds/scans/tears down test clusters)
requirements.txt      # Python dependencies
.env.example          # Environment variable template
```

## License

Apache 2.0 -- see [LICENSE](LICENSE).
