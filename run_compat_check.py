#!/usr/bin/env python3
"""MongoDB Atlas -> Amazon DocumentDB compatibility scanner and report generator.

This is the main entry point for assessing MongoDB Atlas clusters for
compatibility with Amazon DocumentDB.  It orchestrates three open-source
scanning tools plus a custom live-cluster feature scan, then generates a
single consolidated assessment report.

FULL SCAN PIPELINE (ASCII Diagram)
===================================

The diagram below shows everything that happens when you run this script.
Each numbered box is a "step" function in this file.  Data flows top to
bottom; arrows show what each step produces and what later steps consume.

::

    +----------------------------------------------------------+
    |                    main() / entry point                   |
    |  - Loads .env credentials                                |
    |  - Discovers Atlas projects via AtlasOrgAPI               |
    |  - Iterates over projects, calling _scan_project() each   |
    +------------------------------+---------------------------+
                                   |
                   for each project|
                                   v
    +----------------------------------------------------------+
    |  _scan_project(api, project_name, target_cluster)        |
    |  - Lists clusters in the project via Atlas API            |
    |  - Creates a temp read-only DB user (step 3)              |
    |  - Iterates clusters, calling scan_cluster() for each     |
    |  - Cleans up (deletes temp user) in finally block         |
    +------------------------------+---------------------------+
                                   |
                   for each cluster|
                                   v
    +----------------------------------------------------------+
    |                scan_cluster(api, cluster, uri)            |
    |  Runs ALL four checks below in sequence, then merges     |
    |  all remediation items into one list per cluster.         |
    +------------------------------+---------------------------+
                |          |           |            |
                v          v           v            v
    +----------+  +--------+  +--------+  +-----------------+
    | Step 4   |  | Step 5 |  | Step 6 |  | Step 7          |
    | compat   |  | compat |  | index  |  | feature_scan    |
    | -tool    |  | -tool  |  | -tool  |  | (PyMongo live)  |
    | (URI)    |  | (logs) |  |        |  |                 |
    +----+-----+  +----+---+  +----+---+  +--------+--------+
         |             |           |                |
         v             v           v                v
    compat_uri    compat_logs   index_issues    feature_scan
    _report.txt   _report.txt  _report.txt     _report.txt
         |             |           |                |
         +------+------+-----------+--------+-------+
                |                           |
                v                           v
    +-------------------+       +-------------------------+
    | _build_operator   |       | remediation_items list  |
    | _remediation      |       | (from feature scan)     |
    | _items()          |       +------------+------------+
    | (parses compat    |                    |
    |  reports for ops) |                    |
    +--------+----------+                    |
             |        +----------------------+
             v        v
    +----------------------------------------------------------+
    |  step_summary()  (Step 8)                                |
    |  Concatenates all 4 sub-reports into one cluster file    |
    |  -> reports/<cluster>/summary.txt                        |
    +---------------------------+------------------------------+
                                |
          after ALL clusters    |
          in ALL projects       |
                                v
    +----------------------------------------------------------+
    |  _write_project_summary()                                |
    |  The "one file you send to your boss" -- merges every    |
    |  cluster's results into a single org-wide report with:   |
    |                                                          |
    |    1. Migration Readiness  (ready / code / service)      |
    |    2. Executive Summary    (counts + operator table)     |
    |    3. Remediation Plan     (project > cluster > db)      |
    |    4. Detailed Scan Output (raw tool output per cluster) |
    |                                                          |
    |  -> reports/project_summary.txt                          |
    +----------------------------------------------------------+


Relationship Between Files
==========================

This project consists of three Python files that work together:

::

    +------------------------------------------------------+
    |                  atlas_api.py                         |
    |  Handles ALL communication with the MongoDB Atlas    |
    |  Admin API (v2).  Two auth strategies:               |
    |    - AtlasAPI:      project-scoped, Digest auth      |
    |    - AtlasOrgAPI:   org-scoped,     OAuth2 Bearer    |
    |    - _OrgScopedAPI: adapter so org creds can act     |
    |                     like project-scoped AtlasAPI      |
    |                                                      |
    |  Used by both run_compat_check.py AND                |
    |  setup_test_env.py for every Atlas REST call.        |
    +---+--------------------+-----------------------------+
        |                    |
        | imports            | imports
        v                    v
    +-------------------+   +------------------------------+
    | run_compat_check  |   | setup_test_env.py            |
    | .py (THIS FILE)   |   |                              |
    |                   |   | TEST HARNESS that:           |
    | PRODUCTION entry  |   |   1. Creates Atlas projects  |
    | point.  Scans     |   |      + clusters              |
    | real clusters     |   |   2. Seeds incompatible data |
    | and generates     |   |   3. Calls scan_cluster()    |
    | the report.       |   |      and _write_project      |
    |                   |<--+      _summary() from this    |
    | Can be called:    |   |      file to run the scan    |
    |  - Standalone     |   |   4. Tears everything down   |
    |    (python run_.py|   |                              |
    |  - By test harness|   | Use: validates that the      |
    |    (setup_test_env|   | scanner catches every known  |
    |     imports it)   |   | incompatibility category.    |
    +-------------------+   +------------------------------+

In short:
  - ``atlas_api.py``        = the HTTP client layer  (talks to Atlas)
  - ``run_compat_check.py`` = the scan + report layer (THIS file)
  - ``setup_test_env.py``   = the test harness        (creates fake workloads,
                              imports from this file, then runs the scanner)


Scan Pipeline (Details)
=======================

For each cluster, the following checks run in sequence:

    1. **compat-tool (URI)** -- connects to the live cluster via its
       connection string and identifies unsupported query operators by
       examining the profiler / slow-query log.

    2. **compat-tool (logs)** -- downloads Atlas server logs via the Admin
       API and scans them offline for unsupported operators.  Catches
       operators that were executed outside the profiler window.

    3. **index-tool** -- dumps all indexes and flags those incompatible
       with DocumentDB (e.g., text indexes, wildcard indexes, partial
       filter expressions using unsupported operators).

    4. **Feature scan (live)** -- connects via PyMongo and inspects
       collection metadata for DocumentDB-incompatible features:
       time-series, GridFS, capped collections, clustered indexes,
       client-side encryption, change-stream pre/post images, schema
       validators, views with unsupported pipeline stages, server-side
       JavaScript, and sharded collections.


Report Output
=============

Results are written to ``reports/<cluster_name>/`` with per-tool reports
and a consolidated ``reports/project_summary.txt``.

::

    reports/
    +-- <cluster_name>/
    |   +-- compat_uri_report.txt       <-- Step 4 output
    |   +-- compat_logs_report.txt      <-- Step 5 output
    |   +-- atlas_logs/                 <-- Raw downloaded logs (Step 5)
    |   |   +-- <hostname>_<port>.log
    |   +-- index_dump/                 <-- Raw index JSON (Step 6)
    |   +-- index_issues_report.txt     <-- Step 6 output
    |   +-- feature_scan_report.txt     <-- Step 7 output
    |   +-- summary.txt                 <-- Step 8: all 4 above concatenated
    |
    +-- <another_cluster>/
    |   +-- ...
    |
    +-- project_summary.txt             <-- THE final report for all projects
                                            (Migration Readiness, Executive
                                             Summary, Remediation Plan,
                                             Detailed Scan Output)

The project_summary.txt includes:

    - Migration readiness breakdown (ready / code-changes / service-swap)
    - Unsupported operator inventory with migration paths
    - Per-project, per-cluster, per-database remediation plan
    - Effort estimates in developer-hours (and optional dollar cost)


Usage
=====

::

    python run_compat_check.py                          # scan everything
    python run_compat_check.py --project MyProject      # one project only
    python run_compat_check.py --cluster Cluster0       # one cluster only
    python run_compat_check.py --project P --cluster C  # one cluster in one project

Environment Variables (in .env)
-------------------------------
Required:
    atlas_organization_Client_ID
                            Service account client ID
    atlas_organization_Client_Secret
                            Service account client secret

Optional:
    ATLAS_LOG_DAYS          Days of logs to download (default: 7)
    ENGINEER_HOURLY_RATE    Hourly rate for cost estimates (default: 0 = omit)

CLI Arguments:
    --project NAME          Scan only this project (omit to scan all)
    --cluster NAME          Scan only this cluster (omit to scan all)

Dependencies
------------
- aws-docdb-tools repo (cloned automatically on first run)
- pymongo[srv], python-dotenv, requests (see requirements.txt)
"""

import gzip
import os
import secrets
import string
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

from atlas_api import AtlasOrgAPI, _OrgScopedAPI, TEMP_USER  # noqa: F401

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
TOOLS_DIR = BASE_DIR / "aws-docdb-tools"
RESULTS_DIR = BASE_DIR / "reports"
COMPAT_TOOL = TOOLS_DIR / "compat-tool" / "compat.py"
INDEX_TOOL = (
    TOOLS_DIR / "index-tool" / "migrationtools" / "documentdb_index_tool.py"
)
DOCDB_VERSION = "8.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _banner(msg: str) -> None:
    """Print a visually distinct section banner to stdout.

    Args:
        msg: Banner text to display.
    """
    line = "=" * 60
    print(f"\n{line}\n  {msg}\n{line}")


def _redact_uri(text: str) -> str:
    """Replace credentials in MongoDB URIs with '***' for safe logging.

    Args:
        text: String that may contain mongodb:// or mongodb+srv:// URIs.

    Returns:
        Text with credentials replaced by ``***:***``.
    """
    import re
    return re.sub(
        r"mongodb(\+srv)?://[^:]+:[^@]+@",
        r"mongodb\1://***:***@",
        text,
    )


def _run(cmd: list[str], output_file: Path | None = None) -> str:
    """Execute a subprocess command and capture its output.

    Prints the command (with redacted URIs), runs it, and optionally saves
    the combined stdout+stderr to a file.

    Args:
        cmd: Command and arguments as a list of strings.
        output_file: If provided, write combined output to this path.

    Returns:
        Combined stdout + stderr as a string.
    """
    print(f"  -> {_redact_uri(' '.join(cmd))}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    combined = result.stdout + ("\n" + result.stderr if result.stderr else "")
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(combined, encoding="utf-8")
        print(f"  -> saved to {output_file}")
    if result.returncode != 0:
        print(f"  [warn] process exited with code {result.returncode}")
    return combined


def _clean_compat_output(text: str) -> str:
    """Strip verbose/noisy sections from compat-tool output for the report.

    The compat-tool produces a lot of per-file detail (filename + line number
    for each unsupported operator occurrence) that clutters the summary
    report.  This function removes those sections while preserving the
    summary tables and important warnings.

    Args:
        text: Raw compat-tool output text.

    Returns:
        Cleaned text with noisy sections removed.
    """
    lines = text.splitlines()
    cleaned: list[str] = []
    skip_block = False
    for line in lines:
        # Skip the per-file breakdown of unsupported operators
        if line.strip().startswith("Unsupported operators by filename and line number"):
            skip_block = True
            continue
        # End the skip block when we hit a summary section header
        if skip_block:
            if (line.strip() == ""
                    or line.strip().startswith("The following")
                    or line.strip().startswith("No unsupported")
                    or line.strip().startswith("WARNING")
                    or line.strip().startswith("Processed")
                    or line.strip().startswith("List of skipped")):
                skip_block = False
            else:
                continue
        # Skip per-file processing progress lines
        if line.strip().startswith("processing file "):
            continue
        # Skip the noisy warning emitted when no supported operators are found
        if "WARNING - No supported operators found" in line:
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def _generate_password(length: int = 32) -> str:
    """Generate a cryptographically random alphanumeric password.

    Args:
        length: Password length (default 32).

    Returns:
        Random password string using [a-zA-Z0-9].
    """
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _build_uri(host: str, user: str, password: str) -> str:
    """Build a MongoDB SRV connection URI with URL-encoded credentials.

    Args:
        host: Cluster hostname (without ``mongodb+srv://`` prefix).
        user: Database username.
        password: Database password.

    Returns:
        Full ``mongodb+srv://`` URI string.
    """
    return f"mongodb+srv://{quote_plus(user)}:{quote_plus(password)}@{host}/"


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def step_clone_tools() -> None:
    """Clone the aws-docdb-tools repo (compat-tool + index-tool) if not already present."""
    _banner("Step 1: Clone AWS DocumentDB tools")
    if TOOLS_DIR.exists() and (TOOLS_DIR / ".git").exists():
        print("  Repo already cloned – skipping.")
        return
    subprocess.run(
        [
            "git", "clone", "--depth", "1",
            "https://github.com/awslabs/amazon-documentdb-tools.git",
            str(TOOLS_DIR),
        ],
        check=True,
    )
    print("  Clone complete.")


def step_install_deps() -> None:
    """Install Python dependencies from both the tools repo and this project."""
    _banner("Step 2: Install dependencies")
    req_file = TOOLS_DIR / "index-tool" / "requirements.txt"
    if req_file.exists():
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", "-r", str(req_file)],
            check=True,
        )
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q", "-r",
         str(BASE_DIR / "requirements.txt")],
        check=True,
    )
    print("  Dependencies installed.")


def step_create_temp_user(api: AtlasAPI) -> str:
    """Create a temporary read-only user. Returns the password."""
    _banner("Step 3: Create temporary database user")
    password = _generate_password()
    api.create_temp_user(password)
    print(f"  Created temp user '{TEMP_USER}' with readAnyDatabase role.")
    print("  Waiting for user to propagate across cluster nodes...")
    time.sleep(10)
    return password


def step_compat_uri(uri: str, cluster_dir: Path) -> str:
    """Run compat-tool against a live cluster URI to find unsupported operators.

    Args:
        uri: MongoDB connection URI for the cluster.
        cluster_dir: Directory to write the report to.

    Returns:
        Cleaned compat-tool output text.
    """
    _banner("Step 4: Compat-tool – live URI check")
    out_file = cluster_dir / "compat_uri_report.txt"
    raw = _run(
        [sys.executable, str(COMPAT_TOOL), "--uri", uri, "--version", DOCDB_VERSION],
    )
    cleaned = _clean_compat_output(raw)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(cleaned, encoding="utf-8")
    return cleaned


def step_compat_logs(api: AtlasAPI, cluster_name: str, cluster_dir: Path) -> str:
    """Download Atlas server logs via the Admin API and scan them with compat-tool.

    Downloads gzip-compressed logs from each mongod process in the cluster,
    decompresses them, then runs compat-tool in directory mode.  Also picks
    up any mock log files placed in the atlas_logs directory (used by the
    test harness).

    Args:
        api: Atlas API client (AtlasAPI or _OrgScopedAPI).
        cluster_name: Name of the cluster to download logs for.
        cluster_dir: Directory to write logs and report to.

    Returns:
        Cleaned compat-tool output text, or a warning message if no logs.
    """
    _banner("Step 5: Compat-tool – log scan (Atlas Admin API)")

    logs_dir = cluster_dir / "atlas_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_days = int(os.environ.get("ATLAS_LOG_DAYS", "7"))
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - (log_days * 24 * 60 * 60 * 1000)
    print(f"  Log window: last {log_days} day(s)")

    print("  Fetching cluster process list...")
    processes = api.list_processes()
    cluster_processes = [
        p for p in processes
        if p.get("userAlias", "").startswith(cluster_name)
        or cluster_name in p.get("hostname", "")
    ]
    if not cluster_processes:
        cluster_processes = processes
    print(f"  Found {len(cluster_processes)} process(es) for '{cluster_name}'")

    downloaded = 0
    for proc in cluster_processes:
        hostname = proc["hostname"]
        port = proc["port"]
        host_id = f"{hostname}:{port}"
        # Atlas v2 log endpoint uses hostname only (no port) in the path
        log_path_api = f"/clusters/{hostname}/logs/mongodb.gz?startDate={start_ms}&endDate={end_ms}"
        print(f"  Downloading logs from {host_id}...")
        log_resp = api.get(log_path_api, accept=api.GZIP_ACCEPT, stream=True)

        if log_resp.status_code == 200:
            gz_path = logs_dir / f"{hostname}_{port}.log.gz"
            gz_path.write_bytes(log_resp.content)
            log_path = logs_dir / f"{hostname}_{port}.log"
            with gzip.open(gz_path, "rb") as gz_in:
                log_path.write_bytes(gz_in.read())
            gz_path.unlink()
            size_kb = log_path.stat().st_size / 1024
            print(f"    -> {log_path.name} ({size_kb:.0f} KB)")
            downloaded += 1
        else:
            print(f"    [warn] HTTP {log_resp.status_code}: {log_resp.text[:200]}")

    # Inject mock log entries if provided (for test environments)
    mock_log_file = cluster_dir / "atlas_logs" / "_mock_workload.log"
    if mock_log_file.exists():
        print(f"  Found mock workload log ({mock_log_file.stat().st_size / 1024:.0f} KB)")
        downloaded = max(downloaded, 1)  # ensure we proceed to scan

    if downloaded == 0:
        msg = "  [warn] No logs downloaded (may be a tenant/free-tier cluster)."
        print(msg)
        report = cluster_dir / "compat_logs_report.txt"
        report.write_text(msg, encoding="utf-8")
        return msg

    print(f"  Downloaded {downloaded} log file(s). Running compat-tool...")
    out_file = cluster_dir / "compat_logs_report.txt"
    raw = _run(
        [sys.executable, str(COMPAT_TOOL), "--directory", str(logs_dir),
         "--version", DOCDB_VERSION],
    )
    cleaned = _clean_compat_output(raw)
    out_file.write_text(cleaned, encoding="utf-8")
    return cleaned


def step_index_tool(uri: str, cluster_dir: Path) -> str:
    """Dump all indexes from the cluster and check for DocumentDB incompatibilities.

    Runs in two phases:
    1. Dump all indexes from the live cluster to local JSON files.
    2. Analyze the dumped indexes for issues (unsupported index types, etc.).

    Args:
        uri: MongoDB connection URI for the cluster.
        cluster_dir: Directory to write index dump and report to.

    Returns:
        Index-tool issues report text.
    """
    _banner("Step 6: Index-tool – dump & check")
    index_dir = cluster_dir / "index_dump"
    index_dir.mkdir(parents=True, exist_ok=True)

    print("  Phase 1: dumping indexes...")
    _run([
        sys.executable, str(INDEX_TOOL),
        "--uri", uri, "--dump-indexes", "--dir", str(index_dir),
    ])

    print("  Phase 2: checking for issues...")
    out_file = cluster_dir / "index_issues_report.txt"
    return _run([
        sys.executable, str(INDEX_TOOL),
        "--show-issues", "--support-2dsphere", "--dir", str(index_dir),
    ], output_file=out_file)


def step_feature_scan(uri: str, cluster_dir: Path) -> tuple[str, list[dict]]:
    """Scan the live cluster for DocumentDB-unsupported features.

    Returns (report_text, remediation_items) where each remediation item is::

        {"category": str, "db": str, "affected": [str],
         "fix": str, "hours": float}
    """
    _banner("Step 7: Feature compatibility scan (live cluster)")
    from pymongo import MongoClient

    findings: list[str] = []
    warnings = 0
    # Each remediation item tracks db so we can roll up by db / cluster / project
    remediation_items: list[dict] = []

    def _warn(msg: str) -> None:
        nonlocal warnings
        warnings += 1
        findings.append(f"  [!!] {msg}")
        print(f"  [!!] {msg}")

    def _remediation(msg: str) -> None:
        findings.append(f"       -> FIX: {msg}")
        print(f"       -> FIX: {msg}")

    def _ok(msg: str) -> None:
        findings.append(f"  [ok] {msg}")
        print(f"  [ok] {msg}")

    def _info(msg: str) -> None:
        findings.append(f"  [info] {msg}")
        print(f"  [info] {msg}")

    client = MongoClient(uri)

    system_dbs = {"admin", "local", "config"}
    all_dbs = [d for d in client.list_database_names() if d not in system_dbs]
    _info(f"Databases to scan: {all_dbs}")

    # Helper: extract db name from "dbname.collname" strings
    def _db_of(qualified: str) -> str:
        return qualified.split(".")[0] if "." in qualified else "(cluster-wide)"

    # Helper: add a remediation item with per-db tracking
    def _add_item(category: str, affected: list[str], fix: str, hours: float,
                  db: str = "") -> None:
        """Record a remediation item.  *db* defaults to the common prefix of
        *affected* (split on '.') or '(cluster-wide)'."""
        if not db:
            dbs = {_db_of(a) for a in affected}
            db = dbs.pop() if len(dbs) == 1 else "(multiple)"
        remediation_items.append({
            "category": category,
            "db": db,
            "affected": list(affected),
            "fix": fix,
            "hours": hours,
        })

    # 0. Sharded collections
    print("\n  Checking for sharded collections...")
    sharded_found = []
    try:
        config_colls = client["config"]["collections"].find({"dropped": {"$ne": True}})
        for doc in config_colls:
            ns = doc.get("_id", "")
            db_part = ns.split(".")[0] if "." in ns else ""
            if db_part and db_part not in system_dbs:
                shard_key = doc.get("key", {})
                sharded_found.append(f"{ns} (key: {shard_key})")
    except Exception:
        # config.collections may not exist on replica-set clusters
        pass
    if sharded_found:
        _warn(f"Sharded collections found (DocDB uses elastic clusters differently): {sharded_found}")
        _remediation(
            "DocumentDB elastic clusters support hash-based sharding via a "
            "shard key, but NOT range-based shard keys or zone/tag-aware "
            "sharding. Options: (A) Migrate to DocDB elastic clusters -- "
            "choose a high-cardinality shard key (hashed). (B) If the data "
            "fits on a single instance, drop to a non-sharded DocDB cluster "
            "and rely on vertical scaling + read replicas. (C) For "
            "geographically partitioned data, use separate DocDB clusters "
            "per region instead of zone sharding."
        )
        for item in sharded_found:
            ns = item.split(" ")[0]
            _add_item(
                "Sharded Collection",
                [ns],
                "Evaluate for DocDB elastic clusters (hash shard key only) "
                "or consolidate onto a single DocDB instance if data size allows. "
                "Remove zone/tag-aware sharding config",
                12.0,
                db=_db_of(ns),
            )
    else:
        _ok("No sharded collections found (replica-set topology).")

    # 1. Time-Series collections
    print("\n  Checking for time-series collections...")
    ts_found = []
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            coll_type = coll_info.get("type", "")
            options = coll_info.get("options", {})
            if coll_type == "timeseries" or "timeseries" in options:
                ts_found.append(f"{db_name}.{coll_info['name']}")
    if ts_found:
        _warn(f"Time-series collections found (NOT supported in DocDB): {ts_found}")
        _remediation(
            "Replace with Amazon Timestream for time-series workloads, or "
            "flatten into a regular DocDB collection with a TTL index on the "
            "timestamp field and compound indexes on (meta, ts). "
            "Update app writes to insert into flat collection instead of "
            "timeseries bucket format."
        )
        for coll in ts_found:
            _add_item(
                "Time-Series Collection",
                [coll],
                "Migrate to Amazon Timestream (native) or flatten into regular "
                "DocDB collection with TTL + compound indexes on (meta, ts)",
                12.0,
            )
    else:
        _ok("No time-series collections found.")

    # 2. GridFS collections
    print("  Checking for GridFS collections...")
    gridfs_found = []
    for db_name in all_dbs:
        for name in client[db_name].list_collection_names():
            if name.endswith(".files") or name.endswith(".chunks"):
                gridfs_found.append(f"{db_name}.{name}")
    if gridfs_found:
        _warn(f"GridFS collections found (NOT supported in DocDB): {gridfs_found}")
        _remediation(
            "Migrate file storage to Amazon S3. Store file metadata in a "
            "regular DocDB collection (filename, size, S3 key, content type). "
            "Update app code to use the AWS SDK (S3 PutObject/GetObject) "
            "instead of pymongo GridFS. Use S3 presigned URLs for direct "
            "client downloads."
        )
        # Group by bucket prefix (e.g. "mydb.fs" from "mydb.fs.files")
        gridfs_buckets: dict[str, list[str]] = {}
        for name in gridfs_found:
            bucket = name.rsplit(".", 1)[0]
            gridfs_buckets.setdefault(bucket, []).append(name)
        for bucket, colls in gridfs_buckets.items():
            _add_item(
                "GridFS File Storage",
                colls,
                "Migrate to Amazon S3 + DocDB metadata collection. "
                "Replace GridFS put/get calls with S3 PutObject/GetObject",
                16.0,
                db=_db_of(bucket),
            )
    else:
        _ok("No GridFS collections found.")

    # 3. Capped collections
    print("  Checking for capped collections...")
    capped_found = []
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            if coll_info.get("options", {}).get("capped"):
                capped_found.append(f"{db_name}.{coll_info['name']}")
    if capped_found:
        _warn(f"Capped collections found (limited support in DocDB): {capped_found}")
        _remediation(
            "Option A: Use a regular DocDB collection with a TTL index to "
            "auto-expire old documents (best for log/event data). "
            "Option B: Use Amazon Kinesis Data Streams for high-throughput "
            "ordered ingestion, then persist to DocDB. "
            "If tailable cursors are used, switch to DocDB change streams."
        )
        for coll in capped_found:
            _add_item(
                "Capped Collection",
                [coll],
                "Replace with regular collection + TTL index, or use "
                "Amazon Kinesis Data Streams. Replace tailable cursors "
                "with change streams",
                6.0,
            )
    else:
        _ok("No capped collections found.")

    # 4. Clustered indexes
    print("  Checking for clustered indexes...")
    clustered_found = []
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            if "clusteredIndex" in coll_info.get("options", {}):
                clustered_found.append(f"{db_name}.{coll_info['name']}")
    if clustered_found:
        _warn(f"Clustered indexes found (NOT supported in DocDB): {clustered_found}")
        _remediation(
            "Recreate as regular collections in DocDB. Add a standard index "
            "on the clustered key field to maintain query performance. "
            "DocDB already stores documents efficiently -- the main loss is "
            "the storage co-location optimization, which rarely matters at "
            "typical document sizes."
        )
        for coll in clustered_found:
            _add_item(
                "Clustered Index",
                [coll],
                "Recreate as regular collection with a standard index on the "
                "clustered key field",
                4.0,
            )
    else:
        _ok("No clustered indexes found.")

    # 5. Client-Side Field-Level Encryption / Queryable Encryption
    print("  Checking for encryption metadata...")
    enc_found = []
    keyvault_found = []
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            options = coll_info.get("options", {})
            name = coll_info["name"]
            if "encryptedFields" in options:
                enc_found.append(f"{db_name}.{name}")
            if "keyvault" in db_name.lower() or "keyvault" in name.lower() \
               or "datakeys" in name.lower() or "key_vault" in name.lower():
                keyvault_found.append(f"{db_name}.{name}")
    if enc_found:
        _warn(f"Collections with encryptedFields (NOT supported in DocDB): {enc_found}")
        _remediation(
            "Use AWS KMS + application-layer encryption. Encrypt sensitive "
            "fields in app code before writing to DocDB using the AWS "
            "Encryption SDK. Store data keys in AWS KMS instead of a "
            "MongoDB key vault collection. Queryable encryption must be "
            "replaced with exact-match lookups on HMAC hashes of the "
            "encrypted values."
        )
        for coll in enc_found:
            _add_item(
                "Client-Side Field Encryption",
                [coll],
                "Replace with AWS KMS + AWS Encryption SDK at the app layer. "
                "Store keys in KMS, encrypt before write, decrypt after read",
                24.0,
            )
    if keyvault_found:
        _warn(f"Possible key-vault collections for CSFLE (NOT supported in DocDB): {keyvault_found}")
        _remediation(
            "Migrate encryption keys from MongoDB key vault to AWS KMS. "
            "Update driver configuration to remove autoEncryption settings."
        )
        if not enc_found:
            for coll in keyvault_found:
                _add_item(
                    "CSFLE Key Vault",
                    [coll],
                    "Migrate encryption keys to AWS KMS, remove MongoDB "
                    "autoEncryption driver config",
                    12.0,
                )
    if not enc_found and not keyvault_found:
        _ok("No client-side encryption metadata found.")

    # 6. Change-stream pre/post images
    print("  Checking for change-stream pre/post image config...")
    prepost_found = []
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            if coll_info.get("options", {}).get("changeStreamPreAndPostImages", {}).get("enabled"):
                prepost_found.append(f"{db_name}.{coll_info['name']}")
    if prepost_found:
        _warn(f"Collections with pre/post images enabled (NOT supported in DocDB): {prepost_found}")
        _remediation(
            "DocDB change streams return the changed document but not "
            "pre-images. Options: (A) Read the full document on each change "
            "event via a lookup (adds latency). (B) Use DynamoDB Streams "
            "with old/new image capture if the use case fits a key-value "
            "pattern. (C) Maintain a before/after audit log collection in "
            "app code by writing the old doc before each update."
        )
        for coll in prepost_found:
            _add_item(
                "Change Stream Pre/Post Images",
                [coll],
                "Remove pre/post image config. Add app-level before/after "
                "capture (read-before-write), or use DynamoDB Streams for "
                "old+new image support",
                8.0,
            )
    else:
        _ok("No change-stream pre/post image configurations found.")

    # 7. system.js (server-side JavaScript / Map-Reduce)
    print("  Checking for server-side JavaScript (system.js)...")
    sysjs_found = []
    for db_name in all_dbs:
        if "system.js" in client[db_name].list_collection_names():
            count = client[db_name]["system.js"].count_documents({})
            if count > 0:
                sysjs_found.append(f"{db_name}.system.js ({count} functions)")
                _add_item(
                    "Server-Side JavaScript (system.js)",
                    [f"{db_name}.system.js"],
                    "Rewrite as aggregation pipelines or app-layer code. "
                    "For heavy analytics offload to AWS Lambda / EMR / Athena",
                    6.0 * count,
                    db=db_name,
                )
    if sysjs_found:
        _warn(f"Server-side JS found (limited Map-Reduce in DocDB): {sysjs_found}")
        _remediation(
            "Rewrite server-side JS functions as application code or as "
            "DocDB aggregation pipelines. Map-Reduce jobs should be "
            "converted to $group/$accumulator aggregation stages. "
            "For heavy analytics, consider AWS Lambda triggered by DocDB "
            "change streams, or offload to Amazon EMR/Athena via S3 export."
        )
    else:
        _ok("No server-side JavaScript (system.js) found.")

    # 8. Schema validation
    print("  Checking for JSON schema validation rules...")
    schema_found = []
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            if "validator" in coll_info.get("options", {}):
                schema_found.append(f"{db_name}.{coll_info['name']}")
    if schema_found:
        _info(f"Collections with validators (verify syntax compatibility): {schema_found}")
        _remediation(
            "DocDB 5.0+ supports $jsonSchema validation with a subset of "
            "keywords. Audit each validator -- remove unsupported keywords "
            "(e.g. $regex patterns in schemas, if/then/else). For complex "
            "validation, move to app-layer validation (e.g. Pydantic, Joi, "
            "Mongoose schemas) before writes."
        )
        for coll in schema_found:
            _add_item(
                "Schema Validator (review needed)",
                [coll],
                "Audit $jsonSchema validator for unsupported keywords. "
                "Simplify or move complex validation to app layer "
                "(Pydantic / Joi / Mongoose)",
                4.0,
            )
    else:
        _ok("No schema validators found.")

    # 9. Read/Write concern defaults
    print("  Checking cluster read/write concern defaults...")
    try:
        rw_defaults = client.admin.command("getDefaultRWConcern")
        default_rc = rw_defaults.get("defaultReadConcern", {})
        default_wc = rw_defaults.get("defaultWriteConcern", {})
        if default_rc:
            level = default_rc.get("level", "")
            if level in ("linearizable", "snapshot"):
                _warn(f"Default read concern '{level}' may behave differently in DocDB")
                _remediation(
                    f"DocDB does not support '{level}' read concern. "
                    f"Switch to 'local' or 'majority'. For strong "
                    f"consistency, use readPreference=primary which is "
                    f"the DocDB default."
                )
                _add_item(
                    "Read Concern Incompatibility",
                    [f"Cluster default: {level}"],
                    f"Change read concern from '{level}' to 'local' or "
                    f"'majority'. Use readPreference=primary for consistency",
                    4.0,
                    db="(cluster-wide)",
                )
            else:
                _info(f"Default read concern: {default_rc}")
        if default_wc:
            _info(f"Default write concern: {default_wc}")
    except Exception as exc:
        _info(f"Could not fetch RW concern defaults: {exc}")

    # 10. Views
    print("  Checking for views...")
    views_found = []
    unsupported_view_stages = []
    # Stages NOT supported in DocDB 8.0 aggregation
    DOCDB_UNSUPPORTED_STAGES = {
        "$graphLookup", "$bucketAuto", "$bucket", "$sortByCount",
        "$facet", "$lookup",  # $lookup with pipeline subquery form
        "$merge", "$out",
        "$setWindowFields", "$densify", "$fill",
    }
    for db_name in all_dbs:
        for coll_info in client[db_name].list_collections():
            if coll_info.get("type") == "view":
                pipeline = coll_info.get("options", {}).get("pipeline", [])
                stages = [list(s.keys())[0] for s in pipeline if isinstance(s, dict)]
                view_name = f"{db_name}.{coll_info['name']}"
                views_found.append(f"{view_name} (stages: {stages})")
                bad_stages = [s for s in stages if s in DOCDB_UNSUPPORTED_STAGES]
                if bad_stages:
                    unsupported_view_stages.append(f"{view_name} uses {bad_stages}")
    if views_found:
        _info(f"Views found (supported in DocDB 8.0, verify pipeline operators): {views_found}")
        if unsupported_view_stages:
            _warn(f"Views with potentially unsupported pipeline stages: {unsupported_view_stages}")
            _remediation(
                "Rewrite view pipelines to use DocDB-supported aggregation "
                "stages. $graphLookup -> flatten hierarchy into a path "
                "field at write time, or do recursive lookups in app code. "
                "$merge/$out -> use app-layer ETL or AWS Lambda + S3. "
                "$facet -> split into multiple aggregation queries."
            )
            for entry in unsupported_view_stages:
                view_ns = entry.split(" uses ")[0]
                _add_item(
                    "View with Unsupported Stages",
                    [view_ns],
                    "Rewrite view pipeline: $graphLookup -> materialized "
                    "paths at write time; $merge/$out -> app-layer ETL; "
                    "$facet -> multiple queries",
                    8.0,
                )
        else:
            _remediation(
                "Views are supported in DocDB 8.0. Test each view to "
                "confirm all aggregation operators work as expected."
            )
    else:
        _ok("No views found.")

    # 12. Collection stats summary
    print("  Collecting database/collection summary...")
    total_colls = 0
    total_docs = 0
    for db_name in all_dbs:
        try:
            stats = client[db_name].command("dbStats")
            total_colls += stats.get("collections", 0)
            total_docs += stats.get("objects", 0)
        except Exception:
            pass
    _info(f"Total user collections: {total_colls}, total documents: {total_docs}")

    client.close()

    # ----- Build the recap / remediation summary (grouped by database) -----
    recap_lines: list[str] = []
    recap_lines.append("")
    recap_lines.append("=" * 60)
    recap_lines.append("  CLUSTER REMEDIATION RECAP")
    recap_lines.append("=" * 60)

    if not remediation_items:
        recap_lines.append("")
        recap_lines.append("  No incompatibilities found -- cluster is DocDB-ready!")
    else:
        # Group items by database
        from collections import OrderedDict
        db_groups: OrderedDict[str, list[dict]] = OrderedDict()
        for item in remediation_items:
            db_groups.setdefault(item["db"], []).append(item)

        cluster_total = 0.0
        item_num = 0

        for db_name, items in db_groups.items():
            db_hours = sum(it["hours"] for it in items)
            cluster_total += db_hours
            recap_lines.append("")
            recap_lines.append(f"  DATABASE: {db_name}  ({len(items)} issue(s), ~{db_hours:.0f}h)")
            recap_lines.append(f"  {'-' * 54}")
            for it in items:
                item_num += 1
                recap_lines.append(f"    {item_num}. {it['category']}")
                recap_lines.append(f"       Affected: {', '.join(it['affected'])}")
                recap_lines.append(f"       Fix:      {it['fix']}")
                recap_lines.append(f"       Effort:   ~{it['hours']:.0f}h")

        recap_lines.append("")
        recap_lines.append("=" * 60)
        recap_lines.append(f"  CLUSTER TOTAL: {len(remediation_items)} issue(s), "
                           f"~{cluster_total:.0f}h estimated dev effort")
        recap_lines.append("=" * 60)

    recap_text = "\n".join(recap_lines)
    findings.append(recap_text)
    print(recap_text)

    header = f"Feature Compatibility Scan -- {warnings} warning(s) found\n"
    report_text = header + "\n".join(findings)
    out_file = cluster_dir / "feature_scan_report.txt"
    out_file.write_text(report_text, encoding="utf-8")
    print(f"\n  -> saved to {out_file}")
    return report_text, remediation_items


def step_summary(cluster_dir: Path, cluster_name: str) -> None:
    """Concatenate all per-tool reports into a single cluster summary file.

    Args:
        cluster_dir: Directory containing the individual report files.
        cluster_name: Name of the cluster (used in the report header).
    """
    _banner(f"Step 8: Summary for '{cluster_name}'")
    parts: list[str] = []
    parts.append(f"DocumentDB {DOCDB_VERSION} Compatibility Report -- {cluster_name}")
    parts.append(f"Generated: {datetime.now().isoformat()}")
    parts.append("")

    report_files = [
        ("Compat-tool (URI check)", cluster_dir / "compat_uri_report.txt"),
        ("Compat-tool (log scan)", cluster_dir / "compat_logs_report.txt"),
        ("Index-tool (issues)", cluster_dir / "index_issues_report.txt"),
        ("Feature scan (live cluster)", cluster_dir / "feature_scan_report.txt"),
    ]

    for label, path in report_files:
        parts.append(f"--- {label} ---")
        if path.exists():
            content = path.read_text(encoding="utf-8").strip()
            parts.append(content if content else "(empty – no issues found)")
        else:
            parts.append("(report not generated)")
        parts.append("")

    summary_text = "\n".join(parts)
    summary_file = cluster_dir / "summary.txt"
    summary_file.write_text(summary_text, encoding="utf-8")
    print(summary_text)
    print(f"\n  Full summary saved to {summary_file}")


def step_cleanup(api: AtlasAPI) -> None:
    """Delete the temporary database user created for scanning.

    Args:
        api: Atlas API client.
    """
    _banner("Cleanup: Delete temporary database user")
    api.delete_temp_user()
    print(f"  Deleted temp user '{TEMP_USER}'.")


# ---------------------------------------------------------------------------
# Migration readiness tiers
# ---------------------------------------------------------------------------
# Clusters are classified into one of three migration tiers based on the
# worst-case issue found.  This drives the "Migration Readiness" section
# of the final report.
#
#   "ready"   -- No incompatibilities.  Migrate directly to DocumentDB.
#   "code"    -- Code-level fixes only (operator rewrites, config changes,
#                index adjustments).  No AWS service changes needed.
#   "service" -- Requires replacing a MongoDB feature with an AWS service
#                (e.g., GridFS -> S3, time-series -> Timestream,
#                CSFLE -> KMS + Encryption SDK).

_CATEGORY_TIER: dict[str, str] = {
    # Code-level fixes
    "Capped Collection": "code",
    "Clustered Index": "code",
    "Server-Side JavaScript (system.js)": "code",
    "Schema Validator (review needed)": "code",
    "View with Unsupported Stages": "code",
    "Read Concern Incompatibility": "code",
    "Sharded Collection": "code",
    # AWS service substitution required
    "Time-Series Collection": "service",
    "GridFS File Storage": "service",
    "Client-Side Field Encryption": "service",
    "CSFLE Key Vault": "service",
    "Change Stream Pre/Post Images": "service",
}


def _classify_cluster_tier(items: list[dict]) -> str:
    """Return the worst-case migration tier for a cluster's remediation items.

    Args:
        items: List of remediation item dicts from scan_cluster().

    Returns:
        One of "ready", "code", or "service".
    """
    if not items:
        return "ready"
    tier = "code"  # default for unknown categories
    for it in items:
        cat = it.get("category", "")
        item_tier = _CATEGORY_TIER.get(cat, "code")
        if item_tier == "service":
            return "service"
    return tier


# ---------------------------------------------------------------------------
# Operator remediation mapping
# ---------------------------------------------------------------------------
# Maps each DocumentDB-unsupported MongoDB operator to a tuple of:
#   (category_name, fix_description, estimated_effort_hours)
#
# Operators found by the compat-tool are looked up here to generate
# categorized remediation items with specific migration guidance.
# Operators NOT in this map get a generic "rewrite in app code" entry.
_OPERATOR_CATEGORIES: dict[str, tuple[str, str, float]] = {
    # Window functions
    "$setWindowFields": ("Window Function", "Rewrite as application-side logic or split into $group + $sort pipeline stages", 6.0),
    "$rank": ("Window Function", "Use $group + $sort + application-side ranking", 4.0),
    "$denseRank": ("Window Function", "Use $group + $sort + application-side ranking", 4.0),
    "$documentNumber": ("Window Function", "Use $group + application-side row numbering", 4.0),
    "$shift": ("Window Function", "Use application-side lag/lead processing", 4.0),
    "$expMovingAvg": ("Window Function", "Calculate moving average in application code", 4.0),
    "$derivative": ("Window Function", "Calculate derivative in application code", 4.0),
    "$integral": ("Window Function", "Calculate integral in application code", 4.0),
    # Time-series operators
    "$densify": ("Time-Series Operator", "Fill gaps in application code or pre-process data before insert", 4.0),
    "$fill": ("Time-Series Operator", "Handle missing values in application code", 4.0),
    "$dateDiff": ("Date Operator", "Use $subtract with date math or compute in application code", 2.0),
    # Trig / math
    "$sin": ("Math Operator", "Move trigonometric calculations to application code", 2.0),
    "$cos": ("Math Operator", "Move trigonometric calculations to application code", 2.0),
    "$tan": ("Math Operator", "Move trigonometric calculations to application code", 2.0),
    "$degreesToRadians": ("Math Operator", "Move angle conversion to application code", 2.0),
    # Graph / lookup
    "$graphLookup": ("Graph Traversal", "Flatten hierarchy into materialized path at write time, or do recursive lookups in app code", 8.0),
    # Faceting / grouping
    "$facet": ("Faceted Search", "Split into multiple aggregation queries executed in parallel", 6.0),
    "$bucketAuto": ("Auto Bucketing", "Use $bucket with explicit boundaries or compute buckets in app code", 4.0),
    "$sortByCount": ("Sort By Count", "Use $group + $sort pipeline stages", 2.0),
    # JS execution
    "$accumulator": ("Server-Side JS", "Rewrite as standard aggregation pipeline stages", 6.0),
    "$function": ("Server-Side JS", "Rewrite as standard aggregation pipeline stages or move to app code", 6.0),
    "$where": ("Server-Side JS", "Rewrite as $expr with standard query operators", 4.0),
    # Output stages
    "$merge": ("Output Stage", "Use application-layer ETL or AWS Lambda triggered by change streams", 6.0),
    "$out": ("Output Stage", "Use application-layer ETL or AWS Lambda triggered by change streams", 6.0),
    # Union
    "$unionWith": ("Cross-Collection Query", "Execute separate queries and merge results in application code", 4.0),
    # Field access
    "$getField": ("Field Access Operator", "Use dot notation or $let for dynamic field access", 2.0),
    # Admin / diagnostic (low priority)
    "$planCacheStats": ("Diagnostic Operator", "Not needed for application logic. Use DocDB CloudWatch metrics instead", 0.0),
    "$queryStats": ("Diagnostic Operator", "Not needed for application logic. Use DocDB CloudWatch metrics instead", 0.0),
    "$listSessions": ("Diagnostic Operator", "Not needed for application logic. Use DocDB CloudWatch metrics instead", 0.0),
    # Size / type checks
    "$bsonSize": ("Data Inspection", "Move document size checks to application code", 2.0),
    "$binarySize": ("Data Inspection", "Move binary size checks to application code", 2.0),
    "$isNumber": ("Type Check", "Use $type operator or validate types in application code", 2.0),
    # N-accumulators
    "$topN": ("N-Accumulator", "Use $sort + $limit or application-side processing", 4.0),
    "$bottomN": ("N-Accumulator", "Use $sort + $limit or application-side processing", 4.0),
    "$firstN": ("N-Accumulator", "Use $sort + $limit or application-side processing", 4.0),
    "$lastN": ("N-Accumulator", "Use $sort + $limit or application-side processing", 4.0),
    "$maxN": ("N-Accumulator", "Use $sort + $limit or application-side processing", 4.0),
    "$minN": ("N-Accumulator", "Use $sort + $limit or application-side processing", 4.0),
    # Sampling
    "$sampleRate": ("Sampling", "Use $sample stage or application-side random sampling", 2.0),
}


def _parse_unsupported_operators(report_text: str) -> list[str]:
    """Extract unsupported operator names from compat-tool output.

    Parses the table format produced by compat-tool, which looks like::

        The following unsupported operators were found:
        $graphLookup | executed 5 times
        $facet       | executed 3 times

    Args:
        report_text: Raw or cleaned compat-tool output.

    Returns:
        List of operator name strings (e.g., ["$graphLookup", "$facet"]).
    """
    import re
    operators: list[str] = []
    in_unsupported = False
    for line in report_text.splitlines():
        stripped = line.strip()
        if "unsupported operators were found" in stripped.lower():
            in_unsupported = True
            continue
        if in_unsupported:
            m = re.match(r"(\$\w+)\s*\|", stripped)
            if m:
                operators.append(m.group(1))
            elif stripped == "" or stripped.startswith("The following") or stripped.startswith("No "):
                in_unsupported = False
    return operators


def _parse_unsupported_operators_with_counts(report_text: str) -> dict[str, int]:
    """Extract unsupported operator names with their occurrence counts.

    Like ``_parse_unsupported_operators`` but also captures the numeric count
    from lines like ``$graphLookup | executed 5 times``.

    Args:
        report_text: Raw or cleaned compat-tool output.

    Returns:
        Dict mapping operator name to total occurrence count.
    """
    import re
    ops: dict[str, int] = {}
    in_unsupported = False
    for line in report_text.splitlines():
        stripped = line.strip()
        if "unsupported operators were found" in stripped.lower():
            in_unsupported = True
            continue
        if in_unsupported:
            m = re.match(r"(\$\w+)\s*\|\s*(?:executed|found)\s+(\d+)\s+time", stripped)
            if m:
                op, count = m.group(1), int(m.group(2))
                ops[op] = ops.get(op, 0) + count
            elif stripped == "" or stripped.startswith("The following") or stripped.startswith("No "):
                in_unsupported = False
    return ops


def _build_operator_remediation_items(cluster_dir: Path) -> list[dict]:
    """Parse compat-tool reports and build remediation items for unsupported operators.

    Reads both the URI and log-based compat-tool reports, extracts all
    unsupported operators, groups them by category (using _OPERATOR_CATEGORIES),
    and returns remediation items that get merged into the cluster's overall
    remediation list.

    Args:
        cluster_dir: Path to the cluster's results directory.

    Returns:
        List of remediation item dicts with category, affected, fix, hours, db.
    """
    all_ops: set[str] = set()
    for filename in ["compat_uri_report.txt", "compat_logs_report.txt"]:
        path = cluster_dir / filename
        if path.exists():
            all_ops.update(_parse_unsupported_operators(path.read_text(encoding="utf-8")))

    if not all_ops:
        return []

    # Group operators by category
    from collections import OrderedDict
    groups: dict[str, list[str]] = OrderedDict()
    for op in sorted(all_ops):
        if op in _OPERATOR_CATEGORIES:
            cat, _fix, _hrs = _OPERATOR_CATEGORIES[op]
        else:
            cat = "Unsupported Operator"
        groups.setdefault(cat, []).append(op)

    items: list[dict] = []
    for cat, ops in groups.items():
        # Use the fix/hours from the first op in the group
        first_op = ops[0]
        if first_op in _OPERATOR_CATEGORIES:
            _, fix, hours = _OPERATOR_CATEGORIES[first_op]
        else:
            fix = "Rewrite using DocDB-supported operators or move to application code"
            hours = 1.0
        # Aggregate hours: use max of group (not sum, since fix is similar)
        max_hours = max(
            _OPERATOR_CATEGORIES.get(op, ("", "", 1.0))[2]
            for op in ops
        )
        # Skip zero-effort diagnostic operators
        if max_hours == 0.0:
            continue
        items.append({
            "category": f"Unsupported {cat}",
            "affected": ops,
            "fix": fix,
            "hours": max_hours,
            "db": "(query workload)",
        })

    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scan_cluster(api: AtlasAPI, cluster_info: dict, uri: str) -> list[dict]:
    """Run all checks against a single cluster.

    Returns the list of remediation items from feature scan + compat tool.
    """
    cluster_name = cluster_info["name"]
    cluster_dir = RESULTS_DIR / cluster_name
    cluster_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'#' * 60}")
    print(f"  Scanning cluster: {cluster_name}")
    print(f"{'#' * 60}")

    step_compat_uri(uri, cluster_dir)
    step_compat_logs(api, cluster_name, cluster_dir)
    step_index_tool(uri, cluster_dir)
    _report_text, remediation_items = step_feature_scan(uri, cluster_dir)

    # Add remediation items for unsupported operators found by compat tool
    operator_items = _build_operator_remediation_items(cluster_dir)
    remediation_items.extend(operator_items)

    step_summary(cluster_dir, cluster_name)
    return remediation_items


def _write_project_summary(data, suffix: str = "") -> None:
    """Write a single comprehensive report -- the one file you send to your boss.

    Accepts either:
      - {cluster_name: [items]}                        (single project / main())
      - {project_name: {cluster_name: [items]}}        (multi-project / setup_test_env)

    Report structure:
      1. Executive summary
      2. Per-project > per-cluster detail (all tool outputs)
      3. Remediation plan grouped by project > cluster > database > issue
      4. Grand totals
    """
    from collections import OrderedDict

    _banner("GENERATING ASSESSMENT REPORT")

    # ------------------------------------------------------------------
    # Normalise input into: {project_name: {cluster_name: [items]}}
    # ------------------------------------------------------------------
    if data and isinstance(next(iter(data.values())), list):
        # Flat format: single project -- wrap it
        normalized: dict[str, dict[str, list[dict]]] = {
            "(default project)": data
        }
    else:
        normalized = data

    # ------------------------------------------------------------------
    # Compute totals
    # ------------------------------------------------------------------
    grand_hours = 0.0
    grand_issues = 0
    num_projects = len(normalized)
    num_clusters = 0
    clusters_clean = 0
    clusters_with_issues = 0

    for proj_clusters in normalized.values():
        for items in proj_clusters.values():
            num_clusters += 1
            if items:
                clusters_with_issues += 1
                grand_issues += len(items)
                grand_hours += sum(it["hours"] for it in items)
            else:
                clusters_clean += 1

    hourly_rate = float(os.environ.get("ENGINEER_HOURLY_RATE", "0"))
    grand_cost = grand_hours * hourly_rate if hourly_rate else 0

    L: list[str] = []
    sep = "=" * 70
    thin = "-" * 70

    # ------------------------------------------------------------------
    # 1. Title & executive summary
    # ------------------------------------------------------------------
    L.append(sep)
    L.append(f"  MongoDB Atlas -> Amazon DocumentDB {DOCDB_VERSION}")
    L.append("  Compatibility Assessment Report")
    L.append(sep)
    L.append(f"  Generated: {datetime.now().isoformat()}")
    L.append("")

    # ------------------------------------------------------------------
    # Migration readiness breakdown
    # ------------------------------------------------------------------
    tier_ready: list[str] = []     # migrate as-is
    tier_code: list[str] = []      # code changes only
    tier_service: list[str] = []   # needs AWS service substitution
    service_reasons: dict[str, set[str]] = {}  # cluster -> set of service categories

    for proj_clusters in normalized.values():
        for cluster_name, items in proj_clusters.items():
            tier = _classify_cluster_tier(items)
            if tier == "ready":
                tier_ready.append(cluster_name)
            elif tier == "service":
                tier_service.append(cluster_name)
                reasons = {it["category"] for it in items
                           if _CATEGORY_TIER.get(it["category"]) == "service"}
                service_reasons[cluster_name] = reasons
            else:
                tier_code.append(cluster_name)

    pct_ready = (len(tier_ready) / num_clusters * 100) if num_clusters else 0
    pct_code = (len(tier_code) / num_clusters * 100) if num_clusters else 0
    pct_service = (len(tier_service) / num_clusters * 100) if num_clusters else 0

    L.append(thin)
    L.append("  MIGRATION READINESS")
    L.append(thin)
    L.append("")
    L.append(f"  Migrate immediately:          "
             f"{len(tier_ready):>3} / {num_clusters} clusters  "
             f"({pct_ready:.0f}%)")
    L.append(f"  Migrate with code changes:    "
             f"{len(tier_code):>3} / {num_clusters} clusters  "
             f"({pct_code:.0f}%)")
    L.append(f"  Requires AWS service swap:    "
             f"{len(tier_service):>3} / {num_clusters} clusters  "
             f"({pct_service:.0f}%)")
    L.append("")
    cost_line = f"  Est. total remediation effort: ~{grand_hours:.0f} hours"
    if grand_cost:
        cost_line += f"  (~${grand_cost:,.0f})"
    L.append(cost_line)
    L.append("")

    if tier_service:
        L.append("  Clusters requiring AWS service substitution:")
        for cn in tier_service:
            reasons = service_reasons.get(cn, set())
            L.append(f"    - {cn}")
            for r in sorted(reasons):
                if "Time-Series" in r:
                    L.append(f"        {r} -> Amazon Timestream")
                elif "GridFS" in r:
                    L.append(f"        {r} -> Amazon S3")
                elif "Encryption" in r or "CSFLE" in r:
                    L.append(f"        {r} -> AWS KMS + Encryption SDK")
                elif "Pre/Post" in r:
                    L.append(f"        {r} -> DynamoDB Streams")
                else:
                    L.append(f"        {r}")
        L.append("")

    # ------------------------------------------------------------------
    # Detailed counts
    # ------------------------------------------------------------------
    L.append(thin)
    L.append("  EXECUTIVE SUMMARY")
    L.append(thin)
    L.append(f"  Projects scanned:       {num_projects}")
    L.append(f"  Clusters scanned:       {num_clusters}")
    L.append(f"  Clusters ready (clean): {clusters_clean}")
    L.append(f"  Clusters with issues:   {clusters_with_issues}")
    L.append(f"  Total issues found:     {grand_issues}")
    L.append(f"  Est. remediation effort: ~{grand_hours:.0f} hours")
    L.append("")

    # Collect all unsupported operators across every cluster with counts
    all_unsupported: dict[str, int] = {}
    for proj_clusters in normalized.values():
        for cluster_name in proj_clusters:
            cluster_dir = RESULTS_DIR / cluster_name
            for filename in ["compat_uri_report.txt", "compat_logs_report.txt"]:
                path = cluster_dir / filename
                if path.exists():
                    for op, count in _parse_unsupported_operators_with_counts(
                            path.read_text(encoding="utf-8")).items():
                        all_unsupported[op] = all_unsupported.get(op, 0) + count

    if all_unsupported:
        # Filter out diagnostic-only operators
        display_ops = {
            op: count for op, count in all_unsupported.items()
            if op not in ("$planCacheStats", "$queryStats", "$listSessions")
        }
        if display_ops:
            L.append(f"  Unsupported operators ({len(display_ops)} unique):")
            L.append(f"    {'Operator':<25s} {'Count':>6s}   DocDB Migration Path")
            L.append(f"    {'-' * 25} {'-' * 6}   {'-' * 35}")
            for op in sorted(display_ops):
                count = display_ops[op]
                info = _OPERATOR_CATEGORIES.get(op)
                if info:
                    fix = info[1]
                else:
                    fix = "Rewrite in application code"
                L.append(f"    {op:<25s} {count:>6d}   {fix}")
            L.append("")

    # ------------------------------------------------------------------
    # 2. Remediation plan -- project > cluster > database > issue
    #    (placed immediately after exec summary for quick reading)
    # ------------------------------------------------------------------
    L.append(sep)
    L.append("  REMEDIATION PLAN")
    L.append(sep)

    if grand_issues == 0:
        L.append("")
        L.append("  No incompatibilities found across any project or cluster.")
        L.append("  All clusters are DocumentDB-ready.")
    else:
        item_num = 0
        for proj_name, proj_clusters in normalized.items():
            proj_issues = sum(len(items) for items in proj_clusters.values())
            proj_hours = sum(
                sum(it["hours"] for it in items)
                for items in proj_clusters.values() if items
            )

            L.append(f"\n  PROJECT: {proj_name}  "
                      f"({proj_issues} issue(s), ~{proj_hours:.0f}h)")
            L.append(f"  {'=' * 60}")

            for cluster_name, items in proj_clusters.items():
                if not items:
                    L.append(f"\n    CLUSTER: {cluster_name}  -- DocDB-ready.")
                    continue

                cluster_hours = sum(it["hours"] for it in items)
                L.append(f"\n    CLUSTER: {cluster_name}  "
                          f"({len(items)} issue(s), ~{cluster_hours:.0f}h)")
                L.append(f"    {'-' * 56}")

                db_groups: OrderedDict[str, list[dict]] = OrderedDict()
                for item in items:
                    db_groups.setdefault(item["db"], []).append(item)

                for db_name, db_items in db_groups.items():
                    db_hours = sum(it["hours"] for it in db_items)
                    L.append(f"\n      DATABASE: {db_name}  "
                              f"({len(db_items)} issue(s), ~{db_hours:.0f}h)")
                    for it in db_items:
                        item_num += 1
                        L.append(f"        {item_num}. [{it['category']}]")
                        L.append(f"           Affected:  "
                                  f"{', '.join(it['affected'])}")
                        L.append(f"           Fix:       {it['fix']}")
                        L.append(f"           Effort:    ~{it['hours']:.0f}h")

                sub_line = f"\n      CLUSTER SUBTOTAL: ~{cluster_hours:.0f}h"
                if hourly_rate:
                    sub_line += f"  (~${cluster_hours * hourly_rate:,.0f})"
                L.append(sub_line)

            proj_sub = f"\n    PROJECT SUBTOTAL: ~{proj_hours:.0f}h"
            if hourly_rate:
                proj_sub += f"  (~${proj_hours * hourly_rate:,.0f})"
            L.append(proj_sub)

    L.append("")
    L.append(sep)
    L.append("  TOTALS")
    L.append(sep)
    L.append(f"  Projects scanned:        {num_projects}")
    L.append(f"  Clusters scanned:        {num_clusters}")
    L.append(f"  Total issues:            {grand_issues}")
    effort_line = f"  Total estimated effort:  ~{grand_hours:.0f} hours"
    if grand_cost:
        effort_line += f"  (~${grand_cost:,.0f})"
    L.append(effort_line)
    L.append("")
    L.append("  NOTE: Effort estimates assume a mid-level developer with some")
    L.append("  MongoDB experience but limited AWS/DocDB familiarity. Actual")
    L.append("  time will vary with codebase size, test coverage, and")
    L.append("  deployment complexity. Estimates include research, code")
    L.append("  changes, and basic unit testing -- not code review, QA,")
    L.append("  or production deployment.")
    L.append(sep)

    # ------------------------------------------------------------------
    # 3. Per-project > per-cluster detail (raw tool output)
    # ------------------------------------------------------------------
    L.append("")
    L.append("")
    L.append(sep)
    L.append("  DETAILED SCAN OUTPUT")
    L.append(sep)

    for proj_name, proj_clusters in normalized.items():
        proj_issues = sum(len(items) for items in proj_clusters.values())
        proj_hours = sum(
            sum(it["hours"] for it in items)
            for items in proj_clusters.values() if items
        )
        proj_status = "ISSUES FOUND" if proj_issues else "READY"

        L.append("")
        L.append(sep)
        L.append(f"  PROJECT: {proj_name}  [{proj_status}]")
        L.append(f"  {len(proj_clusters)} cluster(s), "
                  f"{proj_issues} issue(s), ~{proj_hours:.0f}h")
        L.append(sep)

        for cluster_name, items in proj_clusters.items():
            cluster_dir = RESULTS_DIR / cluster_name
            cluster_hours = sum(it["hours"] for it in items) if items else 0
            status = "ISSUES FOUND" if items else "READY"

            L.append("")
            L.append(f"  {thin}")
            L.append(f"  CLUSTER: {cluster_name}  [{status}]")
            if items:
                L.append(f"  {len(items)} issue(s), ~{cluster_hours:.0f}h")
            L.append(f"  {thin}")

            # Include each sub-report
            report_files = [
                ("Compat-tool (URI check)", "compat_uri_report.txt"),
                ("Compat-tool (log scan)",  "compat_logs_report.txt"),
                ("Index-tool (issues)",     "index_issues_report.txt"),
                ("Feature scan",            "feature_scan_report.txt"),
            ]
            for label, filename in report_files:
                path = cluster_dir / filename
                L.append("")
                L.append(f"    --- {label} ---")
                if path.exists():
                    content = path.read_text(encoding="utf-8").strip()
                    if content:
                        for line in content.splitlines():
                            L.append(f"    {line}")
                    else:
                        L.append("    (empty -- no issues found)")
                else:
                    L.append("    (report not generated)")

            L.append("")

        L.append(f"  PROJECT SUBTOTAL: {proj_issues} issue(s), ~{proj_hours:.0f}h")
        L.append("")

    summary_text = "\n".join(L)
    print(summary_text)

    filename = f"project_summary_{suffix}.txt" if suffix else "project_summary.txt"
    out_file = RESULTS_DIR / filename
    out_file.write_text(summary_text, encoding="utf-8")
    print(f"  -> saved to {out_file}")


def _scan_project(api, project_name: str, target_cluster: str = "") -> tuple[str, dict[str, list[dict]]]:
    """Scan all (or one) cluster(s) in a single project.

    Returns (project_name, {cluster_name: [remediation_items]}).
    """
    _banner(f"Scanning project: {project_name}")
    all_clusters = api.list_clusters()
    if target_cluster:
        clusters = [c for c in all_clusters
                    if c["name"].lower() == target_cluster.lower()]
        if not clusters:
            print(f"  [warn] Cluster '{target_cluster}' not found in project "
                  f"'{project_name}'. Available: {[c['name'] for c in all_clusters]}")
            return project_name, {}
    else:
        clusters = all_clusters
    if not clusters:
        print(f"  No clusters found in project '{project_name}'.")
        return project_name, {}
    print(f"  Will scan {len(clusters)} cluster(s): {[c['name'] for c in clusters]}")

    temp_password = step_create_temp_user(api)
    cluster_items: dict[str, list[dict]] = {}
    try:
        for cluster_info in clusters:
            cluster_name = cluster_info["name"]
            conn_strings = cluster_info.get("connectionStrings", {})
            srv = conn_strings.get("standardSrv", "")
            if not srv:
                print(f"  [warn] No SRV connection string for '{cluster_name}', skipping.")
                continue
            host = srv.replace("mongodb+srv://", "")
            uri = _build_uri(host, TEMP_USER, temp_password)
            items = scan_cluster(api, cluster_info, uri)
            cluster_items[cluster_name] = items
    finally:
        step_cleanup(api)

    return project_name, cluster_items


def main() -> None:
    """Entry point: load config, discover clusters, run all scans, generate report.

    Usage::

        python run_compat_check.py                          # scan all projects & clusters
        python run_compat_check.py --project MyProject      # scan one project, all clusters
        python run_compat_check.py --cluster Cluster0       # scan one cluster across all projects
        python run_compat_check.py --project P --cluster C  # scan one cluster in one project
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="MongoDB Atlas -> DocumentDB compatibility scanner")
    parser.add_argument("--project", default="",
                        help="Scan only this project (omit to scan all)")
    parser.add_argument("--cluster", default="",
                        help="Scan only this cluster (omit to scan all)")
    args = parser.parse_args()

    load_dotenv(BASE_DIR / ".env")

    # Validate org-level credentials
    org_client_id = os.environ.get("atlas_organization_Client_ID", "").strip()
    org_client_secret = os.environ.get("atlas_organization_Client_Secret", "").strip()

    if not org_client_id or not org_client_secret:
        sys.exit("Missing atlas_organization_Client_ID or "
                 "atlas_organization_Client_Secret in .env")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    step_clone_tools()
    step_install_deps()

    _banner("Discovering projects from organization")
    org_api = AtlasOrgAPI(org_client_id, org_client_secret)
    all_projects = org_api.list_projects()
    print(f"  Found {len(all_projects)} project(s): "
          f"{[p['name'] for p in all_projects]}")

    if args.project:
        projects = [p for p in all_projects
                    if p["name"].lower() == args.project.lower()]
        if not projects:
            sys.exit(f"Project '{args.project}' not found. "
                     f"Available: {[p['name'] for p in all_projects]}")
        print(f"  Filtered to project: {args.project}")
    else:
        projects = all_projects

    # all_results: {project_name: {cluster_name: [items]}}
    all_results: dict[str, dict[str, list[dict]]] = {}

    for proj in projects:
        proj_name = proj["name"]
        proj_id = proj["id"]
        proj_api = org_api.as_atlas_api(proj_id)
        _, cluster_items = _scan_project(
            proj_api, proj_name, args.cluster)
        if cluster_items:
            all_results[proj_name] = cluster_items

    # Compute suffix for the summary filename
    if args.cluster:
        summary_suffix = f"c-{args.cluster}"
    elif args.project:
        summary_suffix = f"p-{args.project}"
    else:
        summary_suffix = "all"

    _write_project_summary(all_results, suffix=summary_suffix)

    print(f"\n{'=' * 60}")
    print(f"  All done. Reports are in: {RESULTS_DIR}/")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
