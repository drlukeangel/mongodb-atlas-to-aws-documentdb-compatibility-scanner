#!/usr/bin/env python3
"""Test environment setup -- simulate enterprise multi-cluster Atlas topology.

This script is the **test harness** for the compatibility scanner.  It creates
a realistic multi-project Atlas environment with clusters seeded with every
DocumentDB-incompatible feature, runs the full scan pipeline against them,
and tears everything down afterwards.

What It Creates
---------------
Three Atlas projects, each containing one or more clusters:

    compat-test-web-{run_id}/
        ct-app-{run_id}       M10 replica set: capped, validator, view with
                              $graphLookup, plus unsupported query workload
        ct-clean-{run_id}     M10 replica set: plain CRUD only (sanity check)

    compat-test-analytics-{run_id}/
        ct-ts-{run_id}        M30 sharded: time-series, pre/post images,
                              system.js, sharded collections

    compat-test-platform-{run_id}/
        ct-files-{run_id}     M10 replica set: GridFS collections

Each cluster is seeded with data AND a workload of queries using unsupported
operators, plus mock log files that ensure the compat-tool log scanner fires
on every operator category.

Phases
------
1. Create projects and clusters (Atlas API)
2. Wait for all clusters to reach IDLE state
3. Seed data and run incompatible query workloads
3b. Generate mock MongoDB logs with unsupported operators
4. Run the full compatibility scan pipeline
5. Teardown (delete clusters, users, projects)

Usage
-----
::

    python setup_test_env.py                # full run: create, seed, scan, teardown
    python setup_test_env.py --no-teardown  # keep clusters alive for inspection
    python setup_test_env.py --teardown-only  # just delete compat-test-* resources

Environment Variables (in .env)
-------------------------------
Required (same as run_compat_check.py):
    atlas_public_key, atlas_private_key, atlas_group_id

Required for project creation/deletion:
    atlas_organization_Client_ID, atlas_organization_Client_Secret
"""

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent

from atlas_api import AtlasAPI, AtlasOrgAPI, TEMP_USER  # noqa: F401
from run_compat_check import (
    RESULTS_DIR,
    _banner,
    _build_uri,
    _generate_password,
    _scan_project,
    _write_project_summary,
    scan_cluster,
    step_clone_tools,
    step_install_deps,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_PREFIX = "compat-test-"

# Template: project suffix -> cluster definitions
# Actual names are generated at runtime with a unique run ID
_PROJECT_TEMPLATES = [
    {
        "suffix": "web",
        "clusters": [
            {
                "suffix": "app",
                "desc": "Basic CRUD app (capped, validator, view with $graphLookup)",
                "sharded": False,
            },
            {
                "suffix": "clean",
                "desc": "Clean CRUD app (no incompatibilities -- sanity check)",
                "sharded": False,
            },
        ],
    },
    {
        "suffix": "analytics",
        "clusters": [
            {
                "suffix": "ts",
                "desc": "Sharded time-series / analytics (timeseries, pre/post "
                        "images, system.js, sharded collections)",
                "sharded": True,
            },
        ],
    },
    {
        "suffix": "platform",
        "clusters": [
            {
                "suffix": "files",
                "desc": "File storage (GridFS)",
                "sharded": False,
            },
        ],
    },
]


def _generate_run_id() -> str:
    """Short unique ID for this run, e.g. '0215-1423' (MMdd-HHmm)."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%m%d-%H%M")


def _build_project_defs(run_id: str) -> list[dict]:
    """Generate project definitions with unique names for this test run.

    Applies the run_id suffix to all project and cluster names to avoid
    conflicts with other concurrent test runs.

    Args:
        run_id: Unique run identifier (e.g., "0215-1423").

    Returns:
        List of project definition dicts with project_name and clusters.
    """
    defs = []
    for tmpl in _PROJECT_TEMPLATES:
        proj_name = f"compat-test-{tmpl['suffix']}-{run_id}"
        clusters = []
        for ctmpl in tmpl["clusters"]:
            clusters.append({
                "name": f"ct-{ctmpl['suffix']}-{run_id}",
                "desc": ctmpl["desc"],
                "sharded": ctmpl.get("sharded", False),
            })
        defs.append({"project_name": proj_name, "clusters": clusters})
    return defs

POLL_INTERVAL_S = 30
POLL_TIMEOUT_S = 25 * 60  # 25 minutes (M30 sharded clusters can take ~18 min)

SEED_USER = "compat_test_seed"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect(host: str, password: str):
    """Connect to a cluster using the seed user credentials.

    Args:
        host: Cluster hostname (without mongodb+srv:// prefix).
        password: Password for SEED_USER.

    Returns:
        pymongo.MongoClient instance.
    """
    from pymongo import MongoClient
    uri = _build_uri(host, SEED_USER, password)
    return MongoClient(uri)


def _get_host(cluster_info: dict) -> str:
    """Extract the hostname from an Atlas cluster info document.

    Args:
        cluster_info: Cluster document dict from Atlas API.

    Returns:
        Hostname string, or empty string if no SRV connection string.
    """
    srv = cluster_info.get("connectionStrings", {}).get("standardSrv", "")
    return srv.replace("mongodb+srv://", "")


def _create_db_user(api: AtlasAPI, username: str, password: str,
                    roles: list[dict]) -> None:
    """Create a database user in the project, recreating if it exists."""
    body = {
        "databaseName": "admin",
        "username": username,
        "password": password,
        "roles": roles,
    }
    resp = api.post("/databaseUsers", body)
    if resp.status_code == 409:
        api.delete(f"/databaseUsers/admin/{username}")
        time.sleep(2)
        resp = api.post("/databaseUsers", body)
    resp.raise_for_status()


def _delete_db_user(api: AtlasAPI, username: str) -> None:
    resp = api.delete(f"/databaseUsers/admin/{username}")
    if resp.status_code not in (200, 202, 204, 404):
        print(f"  [warn] Could not delete user '{username}': {resp.status_code}")


# ---------------------------------------------------------------------------
# Phase 1: Create projects and clusters
# ---------------------------------------------------------------------------

def create_projects_and_clusters(org_api: AtlasOrgAPI,
                                 base_group_id: str,
                                 project_defs: list[dict]) -> dict[str, dict]:
    """Create test projects + clusters using the org-level service account.

    Returns {project_name: {"project_id": str, "api": _OrgScopedAPI,
                            "clusters": [cluster_def, ...]}}.
    """
    _banner("Phase 1: Create projects and clusters")

    org_id = org_api.get_org_id_from_project(base_group_id)
    print(f"  Organization ID: {org_id}")

    project_map: dict[str, dict] = {}

    for pdef in project_defs:
        proj_name = pdef["project_name"]
        print(f"\n  Creating project '{proj_name}'...")
        proj = org_api.create_project(proj_name, org_id)
        proj_id = proj["id"]
        proj_api = org_api.as_atlas_api(proj_id)
        print(f"    -> Project ID: {proj_id}")

        # Add 0.0.0.0/0 to IP access list so we can connect from anywhere
        print(f"    Adding IP access list entry (0.0.0.0/0)...")
        ip_resp = proj_api.post("/accessList", [
            {"cidrBlock": "0.0.0.0/0", "comment": "compat-test allow all"}
        ])
        if ip_resp.ok:
            print(f"      -> Access list updated.")
        else:
            print(f"      [warn] IP access list: {ip_resp.status_code}: {ip_resp.text[:200]}")

        project_map[proj_name] = {
            "project_id": proj_id,
            "api": proj_api,
            "cluster_defs": pdef["clusters"],
            "cluster_infos": {},  # filled after IDLE
        }

        for cdef in pdef["clusters"]:
            name = cdef["name"]
            is_sharded = cdef.get("sharded", False)
            cluster_type = "SHARDED" if is_sharded else "REPLICASET"
            num_shards = 2 if is_sharded else 1
            instance_size = "M30" if is_sharded else "M10"
            tag = f" ({cluster_type}, {instance_size})"
            print(f"    Creating cluster '{name}'{tag}...")

            body = {
                "name": name,
                "clusterType": cluster_type,
                "replicationSpecs": [
                    {
                        "numShards": num_shards,
                        "regionConfigs": [
                            {
                                "providerName": "AWS",
                                "regionName": "US_EAST_1",
                                "priority": 7,
                                "electableSpecs": {
                                    "instanceSize": instance_size,
                                    "nodeCount": 3,
                                },
                            }
                        ],
                    }
                ],
            }

            resp = proj_api.post("/clusters", body)
            if resp.status_code == 409:
                print(f"      Cluster '{name}' already exists -- skipping.")
                continue
            if not resp.ok:
                print(f"      [ERROR] {resp.status_code}: {resp.text[:300]}")
                sys.exit(1)
            print(f"      -> Accepted (HTTP {resp.status_code})")

    return project_map


# ---------------------------------------------------------------------------
# Phase 2: Wait for IDLE
# ---------------------------------------------------------------------------

def wait_for_all_clusters(project_map: dict[str, dict]) -> None:
    """Poll until every cluster in every project is IDLE."""
    _banner("Phase 2: Wait for clusters to become IDLE")
    start = time.time()

    # Build pending set: (project_name, cluster_name)
    pending: set[tuple[str, str]] = set()
    for proj_name, pdata in project_map.items():
        for cdef in pdata["cluster_defs"]:
            pending.add((proj_name, cdef["name"]))

    while pending:
        elapsed = time.time() - start
        if elapsed > POLL_TIMEOUT_S:
            sys.exit(f"  [ERROR] Timed out after {POLL_TIMEOUT_S}s. "
                     f"Still waiting on: {pending}")

        for proj_name, cname in list(pending):
            proj_api = project_map[proj_name]["api"]
            try:
                info = proj_api.get_cluster(cname)
            except Exception as exc:
                print(f"  [warn] {proj_name}/{cname}: {exc}")
                continue
            state = info.get("stateName", "UNKNOWN")
            print(f"  {proj_name}/{cname}: {state}  ({elapsed:.0f}s)")
            if state == "IDLE":
                project_map[proj_name]["cluster_infos"][cname] = info
                pending.discard((proj_name, cname))

        if pending:
            print(f"  Waiting {POLL_INTERVAL_S}s...")
            time.sleep(POLL_INTERVAL_S)

    print("  All clusters are IDLE.")


def seed_app_cluster(host: str, password: str) -> None:
    """Seed compat-test-app: capped collection, JSON schema validator, view."""
    print("\n  Seeding compat-test-app...")
    client = _connect(host, password)
    db = client["testapp"]

    # Capped collection
    try:
        db.create_collection("logs", capped=True, size=1048576, max=1000)
        print("    Created capped collection 'logs'")
    except Exception as exc:
        print(f"    [skip] logs: {exc}")

    # JSON schema validator
    try:
        db.create_collection("posts", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["title", "body"],
                "properties": {
                    "title": {"bsonType": "string", "description": "must be a string"},
                    "body": {"bsonType": "string"},
                    "votes": {"bsonType": "int", "minimum": 0},
                },
            }
        })
        print("    Created collection 'posts' with $jsonSchema validator")
    except Exception as exc:
        print(f"    [skip] posts: {exc}")

    # Seed employees for the view
    employees = [
        {"_id": 1, "name": "Alice", "managerId": None},
        {"_id": 2, "name": "Bob", "managerId": 1},
        {"_id": 3, "name": "Charlie", "managerId": 2},
    ]
    try:
        db["employees"].drop()
        db["employees"].insert_many(employees)
        print("    Inserted employees data")
    except Exception as exc:
        print(f"    [skip] employees: {exc}")

    # View with $graphLookup
    try:
        db.command("create", "employee_hierarchy", viewOn="employees", pipeline=[
            {
                "$graphLookup": {
                    "from": "employees",
                    "startWith": "$managerId",
                    "connectFromField": "managerId",
                    "connectToField": "_id",
                    "as": "reportingChain",
                }
            }
        ])
        print("    Created view 'employee_hierarchy' with $graphLookup")
    except Exception as exc:
        print(f"    [skip] employee_hierarchy view: {exc}")

    # ---- Run queries with incompatible operators so they appear in logs ----
    print("    Running incompatible query workload...")
    _run_app_workload(db)

    client.close()
    print("  compat-test-app seeding complete.")


def _run_app_workload(db) -> None:
    """Execute queries using DocDB-unsupported operators to generate log entries."""
    coll = db["employees"]

    # $graphLookup (via the view)
    try:
        list(db["employee_hierarchy"].find().limit(5))
        print("      ran $graphLookup (via view)")
    except Exception as exc:
        print(f"      [skip] $graphLookup: {exc}")

    # $facet
    try:
        list(coll.aggregate([
            {"$facet": {
                "byName": [{"$sortByCount": "$name"}],
                "total": [{"$count": "n"}],
            }}
        ]))
        print("      ran $facet + $sortByCount")
    except Exception as exc:
        print(f"      [skip] $facet: {exc}")

    # $merge (write aggregation results to another collection)
    try:
        list(coll.aggregate([
            {"$project": {"name": 1}},
            {"$merge": {"into": "employees_copy"}},
        ]))
        print("      ran $merge")
    except Exception as exc:
        print(f"      [skip] $merge: {exc}")

    # $out (write aggregation results to another collection)
    try:
        list(coll.aggregate([
            {"$project": {"name": 1}},
            {"$out": "employees_snapshot"},
        ]))
        print("      ran $out")
    except Exception as exc:
        print(f"      [skip] $out: {exc}")

    # $bucket
    try:
        list(coll.aggregate([
            {"$bucket": {
                "groupBy": "$_id",
                "boundaries": [0, 2, 4, 10],
                "default": "other",
            }}
        ]))
        print("      ran $bucket")
    except Exception as exc:
        print(f"      [skip] $bucket: {exc}")

    # $bucketAuto
    try:
        list(coll.aggregate([
            {"$bucketAuto": {"groupBy": "$_id", "buckets": 2}}
        ]))
        print("      ran $bucketAuto")
    except Exception as exc:
        print(f"      [skip] $bucketAuto: {exc}")

    # $unionWith
    try:
        list(coll.aggregate([
            {"$unionWith": "employees"},
            {"$limit": 5},
        ]))
        print("      ran $unionWith")
    except Exception as exc:
        print(f"      [skip] $unionWith: {exc}")

    # $where (server-side JS in queries)
    try:
        list(coll.find({"$where": "this._id > 1"}).limit(5))
        print("      ran $where")
    except Exception as exc:
        print(f"      [skip] $where: {exc}")

    # $accumulator (custom JS accumulator)
    try:
        from bson.code import Code
        list(coll.aggregate([
            {"$group": {
                "_id": None,
                "names": {"$accumulator": {
                    "init": Code("function() { return []; }"),
                    "accumulate": Code("function(state, name) { state.push(name); return state; }"),
                    "accumulateArgs": ["$name"],
                    "merge": Code("function(a, b) { return a.concat(b); }"),
                    "lang": "js",
                }},
            }}
        ]))
        print("      ran $accumulator")
    except Exception as exc:
        print(f"      [skip] $accumulator: {exc}")

    # $function (custom JS expression)
    try:
        from bson.code import Code
        list(coll.aggregate([
            {"$addFields": {
                "greeting": {"$function": {
                    "body": Code("function(name) { return 'Hello ' + name; }"),
                    "args": ["$name"],
                    "lang": "js",
                }},
            }},
            {"$limit": 3},
        ]))
        print("      ran $function")
    except Exception as exc:
        print(f"      [skip] $function: {exc}")

    # $setWindowFields
    try:
        list(coll.aggregate([
            {"$setWindowFields": {
                "sortBy": {"_id": 1},
                "output": {
                    "rowNum": {"$documentNumber": {}},
                },
            }}
        ]))
        print("      ran $setWindowFields + $documentNumber")
    except Exception as exc:
        print(f"      [skip] $setWindowFields: {exc}")

    # $text search (requires text index)
    try:
        coll.create_index([("name", "text")])
        list(coll.find({"$text": {"$search": "Alice"}}).limit(3))
        print("      ran $text")
    except Exception as exc:
        print(f"      [skip] $text: {exc}")


def seed_ts_cluster(host: str, password: str) -> None:
    """Seed compat-test-ts: time-series, pre/post images, system.js, sharded coll."""
    print("\n  Seeding compat-test-ts (sharded cluster)...")
    client = _connect(host, password)
    db = client["testanalytics"]

    # Time-series collection
    try:
        db.create_collection("weather", timeseries={
            "timeField": "ts",
            "metaField": "meta",
            "granularity": "hours",
        })
        print("    Created time-series collection 'weather'")
    except Exception as exc:
        print(f"    [skip] weather: {exc}")

    # Change-stream pre/post images
    try:
        db.create_collection("events")
        db.command("collMod", "events", changeStreamPreAndPostImages={"enabled": True})
        print("    Enabled change-stream pre/post images on 'events'")
    except Exception as exc:
        print(f"    [skip] events pre/post: {exc}")

    # system.js function
    try:
        from bson.code import Code
        db["system.js"].replace_one(
            {"_id": "addNums"},
            {"_id": "addNums", "value": Code("function(x, y) { return x + y; }")},
            upsert=True,
        )
        print("    Inserted system.js function 'addNums'")
    except Exception as exc:
        print(f"    [skip] system.js: {exc}")

    # Sharded collection — enable sharding on the database and shard a collection
    try:
        client.admin.command("enableSharding", "testanalytics")
        print("    Enabled sharding on database 'testanalytics'")
    except Exception as exc:
        print(f"    [skip] enableSharding: {exc}")

    try:
        # Create the collection and an index for the shard key first
        db.create_collection("sensor_data")
        db["sensor_data"].create_index([("device_id", 1)])
        client.admin.command("shardCollection", "testanalytics.sensor_data",
                             key={"device_id": 1})
        # Insert some data so the scanner has something to see
        db["sensor_data"].insert_many([
            {"device_id": f"dev-{i}", "value": i * 1.5, "ts": "2025-01-01"}
            for i in range(20)
        ])
        print("    Sharded collection 'sensor_data' on {device_id: 1}")
    except Exception as exc:
        print(f"    [skip] shardCollection: {exc}")

    # ---- Run queries with incompatible operators ----
    print("    Running incompatible query workload...")
    _run_ts_workload(db)

    client.close()
    print("  compat-test-ts seeding complete.")


def _run_ts_workload(db) -> None:
    """Execute queries using DocDB-unsupported operators on the analytics cluster."""
    from datetime import datetime, timezone

    # Insert some data to query against
    try:
        db["metrics"].drop()
        db["metrics"].insert_many([
            {"sensor": f"s{i}", "value": i * 2.5, "ts": datetime.now(timezone.utc),
             "category": "A" if i % 2 == 0 else "B"}
            for i in range(50)
        ])
    except Exception:
        pass

    coll = db["metrics"]

    # $densify
    try:
        list(coll.aggregate([
            {"$densify": {
                "field": "value",
                "range": {"step": 5, "bounds": "full"},
            }},
            {"$limit": 10},
        ]))
        print("      ran $densify")
    except Exception as exc:
        print(f"      [skip] $densify: {exc}")

    # $fill
    try:
        list(coll.aggregate([
            {"$fill": {
                "sortBy": {"ts": 1},
                "output": {"value": {"method": "linear"}},
            }},
            {"$limit": 10},
        ]))
        print("      ran $fill")
    except Exception as exc:
        print(f"      [skip] $fill: {exc}")

    # $setWindowFields with $rank, $denseRank, $shift
    try:
        list(coll.aggregate([
            {"$setWindowFields": {
                "partitionBy": "$category",
                "sortBy": {"value": 1},
                "output": {
                    "valueRank": {"$rank": {}},
                    "valueDenseRank": {"$denseRank": {}},
                },
            }},
            {"$limit": 10},
        ]))
        print("      ran $setWindowFields + $rank + $denseRank")
    except Exception as exc:
        print(f"      [skip] $setWindowFields: {exc}")

    # $changeStream
    try:
        cursor = coll.watch(max_await_time_ms=100)
        cursor.try_next()
        cursor.close()
        print("      ran $changeStream")
    except Exception as exc:
        print(f"      [skip] $changeStream: {exc}")

    # mapReduce command
    try:
        from bson.code import Code
        db.command(
            "mapReduce", "metrics",
            map=Code("function() { emit(this.category, this.value); }"),
            reduce=Code("function(key, values) { return Array.sum(values); }"),
            out={"inline": 1},
        )
        print("      ran mapReduce")
    except Exception as exc:
        print(f"      [skip] mapReduce: {exc}")

    # $collStats
    try:
        list(coll.aggregate([{"$collStats": {"storageStats": {}}}]))
        print("      ran $collStats")
    except Exception as exc:
        print(f"      [skip] $collStats: {exc}")

    # $planCacheStats
    try:
        list(coll.aggregate([{"$planCacheStats": {}}]))
        print("      ran $planCacheStats")
    except Exception as exc:
        print(f"      [skip] $planCacheStats: {exc}")

    # Trig functions ($sin, $cos, etc.)
    try:
        list(coll.aggregate([
            {"$project": {
                "sinVal": {"$sin": {"$degreesToRadians": "$value"}},
                "cosVal": {"$cos": {"$degreesToRadians": "$value"}},
                "tanVal": {"$tan": {"$degreesToRadians": "$value"}},
            }},
            {"$limit": 5},
        ]))
        print("      ran $sin/$cos/$tan/$degreesToRadians")
    except Exception as exc:
        print(f"      [skip] trig: {exc}")

    # $getField / $setField
    try:
        list(coll.aggregate([
            {"$project": {
                "sensorField": {"$getField": "sensor"},
            }},
            {"$limit": 5},
        ]))
        print("      ran $getField")
    except Exception as exc:
        print(f"      [skip] $getField: {exc}")

    # $dateTrunc / $dateDiff
    try:
        list(coll.aggregate([
            {"$project": {
                "truncated": {"$dateTrunc": {"date": "$ts", "unit": "hour"}},
            }},
            {"$limit": 5},
        ]))
        print("      ran $dateTrunc")
    except Exception as exc:
        print(f"      [skip] $dateTrunc: {exc}")


def seed_files_cluster(host: str, password: str) -> None:
    """Seed compat-test-files: GridFS collections."""
    print("\n  Seeding compat-test-files...")
    client = _connect(host, password)
    db = client["testfiles"]

    # GridFS
    try:
        import gridfs
        fs = gridfs.GridFS(db)
        fs.put(b"test file content for compat check", filename="test.bin")
        print("    Stored GridFS file 'test.bin'")
    except Exception as exc:
        print(f"    [skip] GridFS: {exc}")

    # ---- Run queries with incompatible operators ----
    print("    Running incompatible query workload...")
    _run_files_workload(db)

    client.close()
    print("  compat-test-files seeding complete.")


def _run_files_workload(db) -> None:
    """Execute queries using DocDB-unsupported operators on the files cluster."""
    # Seed some documents to query
    try:
        db["documents"].drop()
        db["documents"].insert_many([
            {"title": f"Doc {i}", "content": f"Content for document {i}",
             "size": i * 100, "tags": ["a", "b"] if i % 2 == 0 else ["c"]}
            for i in range(30)
        ])
    except Exception:
        pass

    coll = db["documents"]

    # $regex operators ($regexFind, $regexFindAll, $regexMatch)
    try:
        list(coll.aggregate([
            {"$project": {
                "match": {"$regexMatch": {"input": "$title", "regex": "Doc [12]"}},
                "found": {"$regexFind": {"input": "$title", "regex": "\\d+"}},
            }},
            {"$limit": 5},
        ]))
        print("      ran $regexMatch/$regexFind")
    except Exception as exc:
        print(f"      [skip] $regex ops: {exc}")

    # $replaceAll / $replaceOne
    try:
        list(coll.aggregate([
            {"$project": {
                "cleaned": {"$replaceAll": {"input": "$content", "find": "document", "replacement": "doc"}},
            }},
            {"$limit": 5},
        ]))
        print("      ran $replaceAll")
    except Exception as exc:
        print(f"      [skip] $replaceAll: {exc}")

    # $binarySize / $bsonSize
    try:
        list(coll.aggregate([
            {"$project": {
                "docSize": {"$bsonSize": "$$ROOT"},
            }},
            {"$limit": 5},
        ]))
        print("      ran $bsonSize")
    except Exception as exc:
        print(f"      [skip] $bsonSize: {exc}")

    # $isNumber
    try:
        list(coll.aggregate([
            {"$project": {
                "sizeIsNum": {"$isNumber": "$size"},
            }},
            {"$limit": 5},
        ]))
        print("      ran $isNumber")
    except Exception as exc:
        print(f"      [skip] $isNumber: {exc}")

    # $top / $topN / $bottom / $bottomN
    try:
        list(coll.aggregate([
            {"$group": {
                "_id": None,
                "top3": {"$topN": {
                    "n": 3,
                    "sortBy": {"size": -1},
                    "output": "$title",
                }},
                "bottom3": {"$bottomN": {
                    "n": 3,
                    "sortBy": {"size": -1},
                    "output": "$title",
                }},
            }}
        ]))
        print("      ran $topN/$bottomN")
    except Exception as exc:
        print(f"      [skip] $topN/$bottomN: {exc}")

    # $firstN / $lastN / $maxN / $minN
    try:
        list(coll.aggregate([
            {"$group": {
                "_id": None,
                "firstThree": {"$firstN": {"input": "$title", "n": 3}},
                "lastThree": {"$lastN": {"input": "$title", "n": 3}},
                "maxThree": {"$maxN": {"input": "$size", "n": 3}},
                "minThree": {"$minN": {"input": "$size", "n": 3}},
            }}
        ]))
        print("      ran $firstN/$lastN/$maxN/$minN")
    except Exception as exc:
        print(f"      [skip] $firstN etc: {exc}")

    # $sampleRate
    try:
        list(coll.aggregate([
            {"$match": {"$sampleRate": 0.5}},
            {"$limit": 5},
        ]))
        print("      ran $sampleRate")
    except Exception as exc:
        print(f"      [skip] $sampleRate: {exc}")


def seed_clean_cluster(host: str, password: str) -> None:
    """Seed compat-test-clean: plain collections only, no incompatibilities."""
    print("\n  Seeding compat-test-clean (sanity check -- should be all green)...")
    client = _connect(host, password)
    db = client["testclean"]

    try:
        db["users"].insert_many([
            {"name": "Alice", "email": "alice@example.com", "role": "admin"},
            {"name": "Bob", "email": "bob@example.com", "role": "user"},
            {"name": "Charlie", "email": "charlie@example.com", "role": "user"},
        ])
        db["users"].create_index("email", unique=True)
        print("    Created 'users' collection with unique index on email")
    except Exception as exc:
        print(f"    [skip] users: {exc}")

    try:
        db["orders"].insert_many([
            {"user": "Alice", "item": "Widget", "qty": 3, "total": 29.97},
            {"user": "Bob", "item": "Gadget", "qty": 1, "total": 49.99},
        ])
        db["orders"].create_index([("user", 1), ("item", 1)])
        print("    Created 'orders' collection with compound index")
    except Exception as exc:
        print(f"    [skip] orders: {exc}")

    client.close()
    print("  compat-test-clean seeding complete.")


# Map cluster name suffix -> seeder function
_SEEDER_BY_SUFFIX = {
    "app": seed_app_cluster,
    "ts": seed_ts_cluster,
    "files": seed_files_cluster,
    "clean": seed_clean_cluster,
}


def _get_seeder(cluster_name: str):
    """Find the seeder function for a cluster based on its name suffix.

    Matches the cluster name against known suffixes (app, ts, files, clean).

    Args:
        cluster_name: Name of the cluster (e.g., "ct-app-0215-1423").

    Returns:
        Seeder function, or None if no match.
    """
    for suffix, fn in _SEEDER_BY_SUFFIX.items():
        if f"-{suffix}-" in cluster_name or cluster_name.endswith(f"-{suffix}"):
            return fn
    return None


def seed_all_projects(project_map: dict[str, dict]) -> None:
    """Create seed users and populate all clusters with test data.

    For each project, creates an atlasAdmin seed user, waits for propagation,
    then calls the appropriate seeder function for each cluster.

    Args:
        project_map: Mapping of project_name -> project data dict containing
                     api, cluster_defs, cluster_infos, etc.
    """
    _banner("Phase 3: Seed data across all projects")
    for proj_name, pdata in project_map.items():
        proj_api = pdata["api"]
        password = _generate_password()
        pdata["seed_password"] = password
        print(f"\n  Project '{proj_name}': creating seed user...")
        _create_db_user(proj_api, SEED_USER, password, [
            {"databaseName": "admin", "roleName": "atlasAdmin"},
        ])
        print(f"    Created seed user '{SEED_USER}'. Waiting for propagation...")
        time.sleep(15)

        for cdef in pdata["cluster_defs"]:
            cname = cdef["name"]
            info = pdata["cluster_infos"].get(cname)
            if not info:
                print(f"    [warn] Cluster '{cname}' not ready -- skipping seed.")
                continue
            host = _get_host(info)
            if not host:
                print(f"    [warn] No SRV host for '{cname}' -- skipping seed.")
                continue
            seeder = _get_seeder(cname)
            if seeder:
                seeder(host, password)
            else:
                print(f"    [warn] No seeder for '{cname}' -- skipping.")


# ---------------------------------------------------------------------------
# Phase 3b: Generate mock MongoDB logs with incompatible operators
# ---------------------------------------------------------------------------

# Mock log entries per cluster type.  The compat tool does plain-text
# scanning, so each line just needs to contain the operator name with a
# word boundary after it.  We use MongoDB 8.0 structured JSON log format.

_MOCK_LOG_TEMPLATE = (
    '{{"t":{{"$date":"{ts}"}},"s":"I","c":"COMMAND","id":51803,'
    '"ctx":"conn123","msg":"Slow query",'
    '"attr":{{"type":"command","ns":"{ns}","command":{{{cmd}}}}}}}'
)

# Each entry: (namespace, command snippet containing unsupported operators)
_APP_LOG_ENTRIES = [
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$graphLookup":{{"from":"employees","startWith":"$managerId","connectFromField":"managerId","connectToField":"_id","as":"chain"}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$facet":{{"byName":[{{"$sortByCount":"$name"}}],"total":[{{"$count":"n"}}]}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$bucket":{{"groupBy":"$_id","boundaries":[0,2,4],"default":"other"}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$bucketAuto":{{"groupBy":"$_id","buckets":2}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$merge":{{"into":"employees_copy"}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$out":"employees_snapshot"}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$unionWith":"employees"}},{{"$limit":5}}]'),
    ("testapp.employees", '"find":"employees","filter":{{"$where":"this._id > 1"}}'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$group":{{"_id":null,"names":{{"$accumulator":{{"init":"function(){{return[]}}","lang":"js"}}}}}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$addFields":{{"greeting":{{"$function":{{"body":"function(n){{return n}}","args":["$name"],"lang":"js"}}}}}}}}]'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$setWindowFields":{{"sortBy":{{"_id":1}},"output":{{"rowNum":{{"$documentNumber":{{}}}}}}}}}}]'),
    ("testapp.employees", '"find":"employees","filter":{{"$text":{{"$search":"Alice"}}}}'),
    ("testapp.employees", '"aggregate":"employees","pipeline":[{{"$listSessions":{{}}}}]'),
    ("testapp.posts", '"aggregate":"posts","pipeline":[{{"$replaceWith":"$body"}}]'),
]

_TS_LOG_ENTRIES = [
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$densify":{{"field":"value","range":{{"step":5,"bounds":"full"}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$fill":{{"sortBy":{{"ts":1}},"output":{{"value":{{"method":"linear"}}}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$setWindowFields":{{"partitionBy":"$category","sortBy":{{"value":1}},"output":{{"r":{{"$rank":{{}}}},"d":{{"$denseRank":{{}}}}}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$setWindowFields":{{"sortBy":{{"ts":1}},"output":{{"shifted":{{"$shift":{{"output":"$value","by":1}}}}}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"s":{{"$sin":"$value"}},"c":{{"$cos":"$value"}},"t":{{"$tan":"$value"}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"rad":{{"$degreesToRadians":"$value"}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"f":{{"$getField":"sensor"}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"trunc":{{"$dateTrunc":{{"date":"$ts","unit":"hour"}}}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"diff":{{"$dateDiff":{{"startDate":"$ts","endDate":"$$NOW","unit":"hour"}}}}}}}}]'),
    ("testanalytics.metrics", '"mapReduce":"metrics","map":"function(){{emit(this.category,this.value)}}","reduce":"function(k,v){{return Array.sum(v)}}"'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$collStats":{{"storageStats":{{}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$planCacheStats":{{}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$changeStream":{{}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"expAvg":{{"$expMovingAvg":{{"input":"$value","N":3}}}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"deriv":{{"$derivative":{{"input":"$value","unit":"hour"}}}}}}}}]'),
    ("testanalytics.metrics", '"aggregate":"metrics","pipeline":[{{"$project":{{"integ":{{"$integral":{{"input":"$value","unit":"hour"}}}}}}}}]'),
]

_FILES_LOG_ENTRIES = [
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"m":{{"$regexMatch":{{"input":"$title","regex":"Doc"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"f":{{"$regexFind":{{"input":"$title","regex":"\\\\d+"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"fa":{{"$regexFindAll":{{"input":"$title","regex":"\\\\d+"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"c":{{"$replaceAll":{{"input":"$content","find":"x","replacement":"y"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"c":{{"$replaceOne":{{"input":"$content","find":"x","replacement":"y"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"sz":{{"$bsonSize":"$$ROOT"}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"sz":{{"$binarySize":"$content"}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$project":{{"n":{{"$isNumber":"$size"}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$group":{{"_id":null,"t":{{"$topN":{{"n":3,"sortBy":{{"size":-1}},"output":"$title"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$group":{{"_id":null,"b":{{"$bottomN":{{"n":3,"sortBy":{{"size":-1}},"output":"$title"}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$group":{{"_id":null,"f":{{"$firstN":{{"input":"$title","n":3}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$group":{{"_id":null,"l":{{"$lastN":{{"input":"$title","n":3}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$group":{{"_id":null,"mx":{{"$maxN":{{"input":"$size","n":3}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$group":{{"_id":null,"mn":{{"$minN":{{"input":"$size","n":3}}}}}}}}]'),
    ("testfiles.documents", '"aggregate":"documents","pipeline":[{{"$match":{{"$sampleRate":0.5}}}},{{"$limit":5}}]'),
]

# Map cluster name suffix -> mock log entries
_MOCK_LOGS_BY_SUFFIX = {
    "app": _APP_LOG_ENTRIES,
    "ts": _TS_LOG_ENTRIES,
    "files": _FILES_LOG_ENTRIES,
    # "clean" intentionally omitted -- no mock logs for sanity check
}


def _get_mock_entries(cluster_name: str) -> list[tuple[str, str]]:
    for suffix, entries in _MOCK_LOGS_BY_SUFFIX.items():
        if f"-{suffix}-" in cluster_name or cluster_name.endswith(f"-{suffix}"):
            return entries
    return []


def generate_mock_logs(project_map: dict[str, dict]) -> None:
    """Pre-create mock MongoDB log files in each cluster's results directory.

    The compat-tool log scanner will find these alongside real downloaded logs.
    """
    _banner("Phase 3b: Generate mock workload logs")
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)

    for proj_name, pdata in project_map.items():
        for cdef in pdata["cluster_defs"]:
            cname = cdef["name"]
            entries = _get_mock_entries(cname)
            if not entries:
                print(f"  {cname}: no mock logs (clean cluster)")
                continue

            logs_dir = RESULTS_DIR / cname / "atlas_logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            mock_file = logs_dir / "_mock_workload.log"

            lines = []
            for i, (ns, cmd) in enumerate(entries):
                # Spread entries across the last 24 hours
                ts = (now - timedelta(hours=23, minutes=i * 5)).isoformat()
                # Duplicate each entry a few times to simulate repeated usage
                for repeat in range(3):
                    ts_r = (now - timedelta(hours=23 - repeat * 4, minutes=i * 5)).isoformat()
                    line = _MOCK_LOG_TEMPLATE.format(ts=ts_r, ns=ns, cmd=cmd)
                    lines.append(line)

            mock_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
            print(f"  {cname}: wrote {len(lines)} mock log entries "
                  f"({mock_file.stat().st_size / 1024:.1f} KB)")

    print("  Mock logs ready.")


# ---------------------------------------------------------------------------
# Phase 4: Run compat checks across all projects
# ---------------------------------------------------------------------------

def run_all_compat_checks(org_api: AtlasOrgAPI,
                          run_id: str = "") -> dict[str, dict[str, list[dict]]]:
    """Discover compat-test projects from the org API and scan them.

    If run_id is given, only projects matching this run are scanned.
    Returns {project_name: {cluster_name: [remediation_items]}}.
    """
    _banner("Phase 4: Discover and scan compat-test projects")

    all_projects = org_api.list_projects()
    if run_id:
        # Only scan projects from this specific run
        test_projects = [p for p in all_projects
                         if p["name"].startswith(PROJECT_PREFIX)
                         and p["name"].endswith(run_id)]
    else:
        test_projects = [p for p in all_projects
                         if p["name"].startswith(PROJECT_PREFIX)]

    if not test_projects:
        print("  [warn] No compat-test-* projects found in the organization.")
        return {}

    print(f"  Found {len(test_projects)} test project(s): "
          f"{[p['name'] for p in test_projects]}")

    all_results: dict[str, dict[str, list[dict]]] = {}

    for proj in test_projects:
        proj_name = proj["name"]
        proj_id = proj["id"]
        proj_api = org_api.as_atlas_api(proj_id)
        _, cluster_items = _scan_project(proj_api, proj_name)
        if cluster_items:
            all_results[proj_name] = cluster_items

    return all_results


# ---------------------------------------------------------------------------
# Phase 5: Teardown
# ---------------------------------------------------------------------------

def teardown_all(org_api: AtlasOrgAPI, project_map: dict[str, dict]) -> None:
    """Delete all test clusters, users, and projects."""
    _banner("Phase 5: Teardown test projects and clusters")

    for proj_name, pdata in project_map.items():
        proj_api = pdata["api"]
        proj_id = pdata["project_id"]
        print(f"\n  Project '{proj_name}' ({proj_id}):")

        # Delete clusters
        try:
            clusters = proj_api.list_clusters()
        except Exception:
            clusters = []
        for c in clusters:
            cname = c["name"]
            print(f"    Deleting cluster '{cname}'...")
            resp = proj_api.delete(f"/clusters/{cname}")
            print(f"      -> HTTP {resp.status_code}")

        # Delete users
        _delete_db_user(proj_api, SEED_USER)
        _delete_db_user(proj_api, TEMP_USER)

        # Delete the project (must wait for clusters to be fully removed)
        print(f"    Deleting project '{proj_name}'...")
        org_api.delete_project(proj_id)

    print("\n  Teardown initiated. Clusters may take a few minutes to fully remove.")
    print("  If project deletion fails (clusters still deleting), run --teardown-only again.")


def teardown_only(org_api: AtlasOrgAPI) -> None:
    """Find and delete all compat-test-* projects and their clusters."""
    _banner("Teardown: finding compat-test-* projects")

    all_projects = org_api.list_projects()
    test_projects = [p for p in all_projects
                     if p["name"].startswith(PROJECT_PREFIX)]

    if not test_projects:
        print("  No compat-test-* projects found.")
        return

    for proj in test_projects:
        proj_name = proj["name"]
        proj_id = proj["id"]
        proj_api = org_api.as_atlas_api(proj_id)
        print(f"\n  Project '{proj_name}' ({proj_id}):")

        # Delete all clusters in this project
        try:
            clusters = proj_api.list_clusters()
        except Exception:
            clusters = []
        for c in clusters:
            cname = c["name"]
            print(f"    Deleting cluster '{cname}'...")
            resp = proj_api.delete(f"/clusters/{cname}")
            print(f"      -> HTTP {resp.status_code}")

        # Delete users
        _delete_db_user(proj_api, SEED_USER)
        _delete_db_user(proj_api, TEMP_USER)

        # Try to delete the project
        print(f"    Deleting project...")
        org_api.delete_project(proj_id)

    print("\n  Teardown complete.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _get_org_api() -> AtlasOrgAPI:
    """Build an AtlasOrgAPI instance from environment variables.

    Requires atlas_organization_Client_ID and atlas_organization_Client_Secret
    to be set in the environment.

    Returns:
        Configured AtlasOrgAPI instance.

    Raises:
        SystemExit: If required env vars are missing.
    """
    client_id = os.environ.get("atlas_organization_Client_ID", "")
    client_secret = os.environ.get("atlas_organization_Client_Secret", "")
    if not client_id or not client_secret:
        sys.exit("Missing atlas_organization_Client_ID / "
                 "atlas_organization_Client_Secret in .env")
    return AtlasOrgAPI(client_id, client_secret)


def main() -> None:
    """Entry point: parse args, run the full test lifecycle or teardown only."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-teardown", action="store_true",
                        help="Keep test clusters alive after scan for manual inspection")
    parser.add_argument("--teardown-only", action="store_true",
                        help="Only delete existing compat-test-* projects/clusters")
    args = parser.parse_args()

    load_dotenv(BASE_DIR / ".env")

    # Org-level service account (for project creation/deletion)
    org_api = _get_org_api()
    base_group_id = os.environ.get("atlas_group_id", "")

    # --teardown-only: just clean up and exit
    if args.teardown_only:
        teardown_only(org_api)
        return

    # Generate unique run ID for this session
    run_id = _generate_run_id()
    project_defs = _build_project_defs(run_id)
    print(f"  Run ID: {run_id}")
    print(f"  Projects: {[p['project_name'] for p in project_defs]}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    step_clone_tools()
    step_install_deps()

    project_map: dict[str, dict] = {}
    try:
        # Phase 1 & 2: Create projects/clusters and wait
        project_map = create_projects_and_clusters(org_api, base_group_id, project_defs)
        wait_for_all_clusters(project_map)

        # Phase 3: Seed
        seed_all_projects(project_map)

        # Phase 3b: Generate mock logs with incompatible operator usage
        generate_mock_logs(project_map)

        # Phase 4: Discover this run's projects from org API and scan them
        all_results = run_all_compat_checks(org_api, run_id)

        _write_project_summary(all_results)

        _banner("All scans complete")
        print(f"  Reports saved to: {RESULTS_DIR}/")
        project_report = RESULTS_DIR / "project_summary.txt"
        if project_report.exists():
            print(f"  -> {project_report}")
    finally:
        # Clean up seed users in all projects
        for pdata in project_map.values():
            try:
                _delete_db_user(pdata["api"], SEED_USER)
            except Exception:
                pass

        # Phase 5: Teardown (unless --no-teardown)
        if args.no_teardown:
            print("\n  --no-teardown: Projects and clusters left running.")
            print("  Run 'python setup_test_env.py --teardown-only' to delete them.")
        else:
            teardown_all(org_api, project_map)


if __name__ == "__main__":
    main()
