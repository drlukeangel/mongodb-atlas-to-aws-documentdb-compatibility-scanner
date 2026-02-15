#!/usr/bin/env python3
"""MongoDB Atlas API client wrappers for Digest and OAuth2 authentication.

This module provides two distinct authentication strategies for the
MongoDB Atlas Admin API v2:

    AtlasAPI        -- Project-scoped client using HTTP Digest Auth.
                       Best for scripts that operate on a single Atlas project
                       with a programmatic API key (public + private key pair).

    AtlasOrgAPI     -- Organization-scoped client using OAuth2 Client Credentials
                       (Service Account Bearer token).  Required for operations
                       that span multiple projects (e.g., creating/deleting
                       projects, listing all projects in an org).

    _OrgScopedAPI   -- Adapter that wraps AtlasOrgAPI to present the same
                       interface as AtlasAPI for a specific project.  Allows
                       scan functions written for AtlasAPI to work transparently
                       with org-level credentials.

Architecture
------------
::

    AtlasOrgAPI (org-wide, Bearer token)
        |
        +-- .as_atlas_api(group_id) --> _OrgScopedAPI (project-scoped facade)
        |                                   |
        |                                   +-- quacks like AtlasAPI
        |
    AtlasAPI (project-scoped, Digest auth)

Both AtlasAPI and _OrgScopedAPI expose the same core methods used by
scan functions in run_compat_check.py:

    .get(path), .post(path, body), .delete(path)
    .list_clusters(), .get_cluster(name), .list_processes()
    .create_temp_user(password), .delete_temp_user()

Usage
-----
Single-project scanning (Digest auth)::

    from atlas_api import AtlasAPI

    api = AtlasAPI(public_key, private_key, group_id)
    clusters = api.list_clusters()

Multi-project scanning (OAuth2 service account)::

    from atlas_api import AtlasOrgAPI

    org_api = AtlasOrgAPI(client_id, client_secret)
    projects = org_api.list_projects()
    for proj in projects:
        proj_api = org_api.as_atlas_api(proj["id"])
        clusters = proj_api.list_clusters()

Environment Variables
---------------------
This module does not load .env itself -- callers are responsible for
calling ``load_dotenv()`` before constructing API instances.

Required for AtlasAPI:
    atlas_public_key, atlas_private_key, atlas_group_id

Required for AtlasOrgAPI:
    atlas_organization_Client_ID, atlas_organization_Client_Secret
"""

import time

import requests
from requests.auth import HTTPDigestAuth

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Username for the temporary read-only database user created during scans.
# This user is created at scan start and deleted on completion.
TEMP_USER = "docdb_compat_scan"


# ============================================================================
# AtlasOrgAPI -- Organization-scoped, OAuth2 Bearer token
# ============================================================================

class AtlasOrgAPI:
    """Organization-level Atlas API client using Service Account OAuth2.

    Uses the Client Credentials grant to obtain a Bearer token, which is
    automatically refreshed when it approaches expiry (60-second buffer).

    This client is NOT scoped to a specific project -- it can list/create/delete
    projects across the entire organization.  For project-scoped operations,
    use :meth:`as_atlas_api` to get an AtlasAPI-compatible wrapper.

    Attributes:
        BASE: Atlas Admin API v2 base URL.
        TOKEN_URL: OAuth2 token endpoint for Atlas Cloud.
        JSON_ACCEPT: Versioned JSON Accept header required by Atlas API v2.

    Example::

        org_api = AtlasOrgAPI("sa_client_id", "sa_client_secret")
        projects = org_api.list_projects()
        for p in projects:
            scoped = org_api.as_atlas_api(p["id"])
            clusters = scoped.list_clusters()
    """

    BASE = "https://cloud.mongodb.com/api/atlas/v2"
    TOKEN_URL = "https://cloud.mongodb.com/api/oauth/token"
    JSON_ACCEPT = "application/vnd.atlas.2023-02-01+json"

    def __init__(self, client_id: str, client_secret: str):
        """Initialize with OAuth2 service account credentials.

        Args:
            client_id: Atlas service account client ID.
            client_secret: Atlas service account client secret.
        """
        self._session = requests.Session()
        self._client_id = client_id
        self._client_secret = client_secret
        self._token: str | None = None
        self._token_expiry: float = 0  # Unix timestamp when token expires

    # -- Token management ---------------------------------------------------

    def _ensure_token(self) -> str:
        """Obtain or refresh the OAuth2 Bearer token.

        Tokens are cached and reused until 60 seconds before expiry.
        The 60-second buffer prevents using a token that expires mid-request.

        Returns:
            Valid Bearer token string.

        Raises:
            requests.HTTPError: If the token endpoint returns an error.
        """
        if self._token and time.time() < self._token_expiry - 60:
            return self._token

        # Request a new token using client_credentials grant
        resp = requests.post(
            self.TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self._client_id, self._client_secret),
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        # Default to 1-hour expiry if the response doesn't include expires_in
        self._token_expiry = time.time() + data.get("expires_in", 3600)
        return self._token

    def _headers(self) -> dict:
        """Build HTTP headers with a valid Bearer token.

        Returns:
            Dict with Accept, Authorization, and Content-Type headers.
        """
        token = self._ensure_token()
        return {
            "Accept": self.JSON_ACCEPT,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    # -- Low-level HTTP methods (not project-scoped) ------------------------

    def get(self, path: str):
        """Send a GET request to the Atlas API (org-level path).

        Args:
            path: API path appended to BASE (e.g., "/groups" to list projects).

        Returns:
            requests.Response object.
        """
        return self._session.get(f"{self.BASE}{path}", headers=self._headers())

    def post(self, path: str, body: dict):
        """Send a POST request to the Atlas API (org-level path).

        Args:
            path: API path appended to BASE.
            body: JSON-serializable request body.

        Returns:
            requests.Response object.
        """
        return self._session.post(
            f"{self.BASE}{path}", headers=self._headers(), json=body
        )

    def delete(self, path: str):
        """Send a DELETE request to the Atlas API (org-level path).

        Args:
            path: API path appended to BASE.

        Returns:
            requests.Response object.
        """
        return self._session.delete(
            f"{self.BASE}{path}", headers=self._headers()
        )

    # -- High-level org helpers ---------------------------------------------

    def get_org_id_from_project(self, group_id: str) -> str:
        """Look up the organization ID that owns a given project.

        Args:
            group_id: Atlas project (group) ID.

        Returns:
            Organization ID string.

        Raises:
            requests.HTTPError: If the project lookup fails.
        """
        resp = self.get(f"/groups/{group_id}")
        resp.raise_for_status()
        return resp.json()["orgId"]

    def list_projects(self) -> list[dict]:
        """List all projects visible to this service account.

        Returns:
            List of project documents (dicts with 'id', 'name', 'orgId', etc.).
        """
        resp = self.get("/groups")
        resp.raise_for_status()
        return resp.json().get("results", [])

    def create_project(self, name: str, org_id: str) -> dict:
        """Create a new Atlas project, or return the existing one if name conflicts.

        Atlas returns HTTP 409 if a project with the same name already exists
        in the organization.  In that case, we look up and return the existing
        project rather than failing.

        Args:
            name: Desired project name.
            org_id: Organization ID to create the project in.

        Returns:
            Project document dict.

        Raises:
            requests.HTTPError: If creation fails for reasons other than conflict.
        """
        resp = self.post("/groups", {"name": name, "orgId": org_id})
        if resp.status_code == 409:
            # Name conflict -- find and return the existing project
            for p in self.list_projects():
                if p["name"] == name:
                    return p
            resp.raise_for_status()  # should not reach here
        resp.raise_for_status()
        return resp.json()

    def delete_project(self, group_id: str) -> None:
        """Delete a project by ID.  Silently ignores 404 (already deleted).

        Args:
            group_id: Project ID to delete.
        """
        resp = self.delete(f"/groups/{group_id}")
        if resp.status_code not in (200, 202, 204, 404):
            print(
                f"  [warn] Could not delete project {group_id}: "
                f"{resp.status_code} {resp.text[:200]}"
            )

    # -- Group-scoped helpers (for projects created by this service account) -

    def group_get(
        self, group_id: str, path: str, accept: str | None = None, **kwargs
    ):
        """GET a project-scoped endpoint using org Bearer auth.

        Args:
            group_id: Target project ID.
            path: Path appended after /groups/{group_id}.
            accept: Override the Accept header (e.g., for gzip log downloads).
            **kwargs: Passed through to requests.get (e.g., stream=True).

        Returns:
            requests.Response object.
        """
        headers = self._headers()
        if accept:
            headers["Accept"] = accept
        return self._session.get(
            f"{self.BASE}/groups/{group_id}{path}", headers=headers, **kwargs
        )

    def group_post(self, group_id: str, path: str, body: dict):
        """POST to a project-scoped endpoint using org Bearer auth.

        Args:
            group_id: Target project ID.
            path: Path appended after /groups/{group_id}.
            body: JSON-serializable request body.

        Returns:
            requests.Response object.
        """
        return self._session.post(
            f"{self.BASE}/groups/{group_id}{path}",
            headers=self._headers(),
            json=body,
        )

    def group_delete(self, group_id: str, path: str):
        """DELETE a project-scoped endpoint using org Bearer auth.

        Args:
            group_id: Target project ID.
            path: Path appended after /groups/{group_id}.

        Returns:
            requests.Response object.
        """
        return self._session.delete(
            f"{self.BASE}/groups/{group_id}{path}", headers=self._headers()
        )

    def as_atlas_api(self, group_id: str) -> "AtlasAPI":
        """Return an AtlasAPI-compatible wrapper scoped to a specific project.

        The returned ``_OrgScopedAPI`` object exposes the same interface as
        ``AtlasAPI`` (get, post, delete, list_clusters, etc.) but uses this
        org's OAuth Bearer token instead of Digest auth.

        Args:
            group_id: Project ID to scope the wrapper to.

        Returns:
            _OrgScopedAPI instance that quacks like AtlasAPI.
        """
        return _OrgScopedAPI(self, group_id)


# ============================================================================
# _OrgScopedAPI -- AtlasAPI-compatible facade backed by AtlasOrgAPI
# ============================================================================

class _OrgScopedAPI:
    """Project-scoped API wrapper that delegates to AtlasOrgAPI Bearer auth.

    This adapter allows scan functions written for ``AtlasAPI`` (Digest auth)
    to work transparently with org-level OAuth2 credentials.  It implements
    the same interface: get(), post(), delete(), list_clusters(), etc.

    This class is not meant to be instantiated directly -- use
    ``AtlasOrgAPI.as_atlas_api(group_id)`` instead.

    Attributes:
        group_id: The Atlas project ID this wrapper is scoped to.
        JSON_ACCEPT: Versioned JSON Accept header.
        GZIP_ACCEPT: Versioned gzip Accept header (for log downloads).
    """

    JSON_ACCEPT = "application/vnd.atlas.2023-02-01+json"
    GZIP_ACCEPT = "application/vnd.atlas.2023-02-01+gzip"

    def __init__(self, org_api: "AtlasOrgAPI", group_id: str):
        """Initialize the wrapper.

        Args:
            org_api: Parent AtlasOrgAPI instance providing Bearer auth.
            group_id: Atlas project (group) ID to scope all requests to.
        """
        self._org = org_api
        self.group_id = group_id

    # -- Core HTTP methods (delegate to org_api group_* methods) ------------

    def get(self, path: str, accept: str | None = None, **kwargs):
        """GET a project-scoped endpoint.

        Args:
            path: Path appended after /groups/{group_id}.
            accept: Optional Accept header override.
            **kwargs: Passed through to requests (e.g., stream=True).

        Returns:
            requests.Response object.
        """
        return self._org.group_get(
            self.group_id, path, accept=accept, **kwargs
        )

    def post(self, path: str, body: dict):
        """POST to a project-scoped endpoint.

        Args:
            path: Path appended after /groups/{group_id}.
            body: JSON-serializable request body.

        Returns:
            requests.Response object.
        """
        return self._org.group_post(self.group_id, path, body)

    def delete(self, path: str):
        """DELETE a project-scoped endpoint.

        Args:
            path: Path appended after /groups/{group_id}.

        Returns:
            requests.Response object.
        """
        return self._org.group_delete(self.group_id, path)

    # -- High-level helpers (same interface as AtlasAPI) --------------------

    def list_clusters(self) -> list[dict]:
        """List all clusters in this project.

        Returns:
            List of cluster document dicts.
        """
        resp = self.get("/clusters")
        resp.raise_for_status()
        return resp.json().get("results", [])

    def get_cluster(self, name: str) -> dict:
        """Get a single cluster by name.

        Args:
            name: Cluster name.

        Returns:
            Cluster document dict.
        """
        resp = self.get(f"/clusters/{name}")
        resp.raise_for_status()
        return resp.json()

    def list_processes(self) -> list[dict]:
        """List all MongoDB processes (mongod/mongos) in this project.

        Used to discover hostnames for log downloads.

        Returns:
            List of process document dicts.
        """
        resp = self.get("/processes")
        resp.raise_for_status()
        return resp.json().get("results", [])

    def create_temp_user(self, password: str) -> dict:
        """Create a temporary read-only database user for scanning.

        The user gets ``readAnyDatabase`` + ``clusterMonitor`` roles, which
        is the minimum required to run compat checks, index dumps, and
        feature scans without modifying any data.

        If the user already exists (HTTP 409), it is deleted and recreated
        with the new password.

        Args:
            password: Password for the temporary user.

        Returns:
            Database user document dict.

        Raises:
            requests.HTTPError: If user creation fails.
        """
        body = {
            "databaseName": "admin",
            "username": TEMP_USER,
            "password": password,
            "roles": [
                {"databaseName": "admin", "roleName": "readAnyDatabase"},
                {"databaseName": "admin", "roleName": "clusterMonitor"},
            ],
        }
        resp = self.post("/databaseUsers", body)
        if resp.status_code == 409:
            # User already exists from a previous interrupted run -- recreate
            print("  Temp user already exists, recreating...")
            self.delete_temp_user()
            time.sleep(2)
            resp = self.post("/databaseUsers", body)
        resp.raise_for_status()
        return resp.json()

    def delete_temp_user(self):
        """Delete the temporary scan user.  Silently ignores 404."""
        resp = self.delete(f"/databaseUsers/admin/{TEMP_USER}")
        if resp.status_code not in (200, 202, 204, 404):
            print(
                f"  [warn] Could not delete temp user: "
                f"{resp.status_code} {resp.text[:200]}"
            )


# ============================================================================
# AtlasAPI -- Project-scoped, HTTP Digest Auth
# ============================================================================

class AtlasAPI:
    """Project-scoped Atlas API client using HTTP Digest authentication.

    This is the primary client for single-project scanning.  It uses a
    programmatic API key pair (public + private key) scoped to a specific
    Atlas project.

    All request paths are automatically prefixed with
    ``/groups/{group_id}/...`` for project-scoped endpoints, or left
    un-prefixed for org-level endpoints (via ``get_org``, ``post_org``,
    ``delete_org`` methods).

    Attributes:
        BASE: Atlas Admin API v2 base URL.
        JSON_ACCEPT: Versioned JSON Accept header.
        GZIP_ACCEPT: Versioned gzip Accept header (for log downloads).
        group_id: The Atlas project (group) ID this client is scoped to.

    Example::

        api = AtlasAPI(public_key, private_key, group_id)
        for cluster in api.list_clusters():
            print(cluster["name"], cluster["stateName"])
    """

    BASE = "https://cloud.mongodb.com/api/atlas/v2"
    JSON_ACCEPT = "application/vnd.atlas.2023-02-01+json"
    GZIP_ACCEPT = "application/vnd.atlas.2023-02-01+gzip"

    def __init__(self, public_key: str, private_key: str, group_id: str):
        """Initialize with Atlas programmatic API key credentials.

        Args:
            public_key: Atlas API public key.
            private_key: Atlas API private key.
            group_id: Atlas project (group) ID.
        """
        self._auth = HTTPDigestAuth(public_key, private_key)
        self._session = requests.Session()
        self._session.auth = self._auth
        self.group_id = group_id

    # -- URL builders -------------------------------------------------------

    def _url(self, path: str) -> str:
        """Build a project-scoped API URL.

        Args:
            path: Path appended after /groups/{group_id}.

        Returns:
            Full URL string.
        """
        return f"{self.BASE}/groups/{self.group_id}{path}"

    def _org_url(self, path: str) -> str:
        """Build an org-level (non-project-scoped) API URL.

        Args:
            path: Path appended after the base URL.

        Returns:
            Full URL string.
        """
        return f"{self.BASE}{path}"

    # -- Project-scoped HTTP methods ----------------------------------------

    def get(self, path: str, accept: str | None = None, **kwargs):
        """GET a project-scoped endpoint.

        Args:
            path: Path appended after /groups/{group_id}.
            accept: Optional Accept header override (e.g., GZIP_ACCEPT for logs).
            **kwargs: Passed through to requests (e.g., stream=True).

        Returns:
            requests.Response object.
        """
        headers = {"Accept": accept or self.JSON_ACCEPT}
        return self._session.get(self._url(path), headers=headers, **kwargs)

    def get_org(self, path: str):
        """GET an org-level endpoint (not scoped to a project).

        Args:
            path: Path appended after the base URL.

        Returns:
            requests.Response object.
        """
        headers = {"Accept": self.JSON_ACCEPT}
        return self._session.get(self._org_url(path), headers=headers)

    def post(self, path: str, body: dict):
        """POST to a project-scoped endpoint.

        Args:
            path: Path appended after /groups/{group_id}.
            body: JSON-serializable request body.

        Returns:
            requests.Response object.
        """
        headers = {
            "Accept": self.JSON_ACCEPT,
            "Content-Type": "application/json",
        }
        return self._session.post(
            self._url(path), headers=headers, json=body
        )

    def post_org(self, path: str, body: dict):
        """POST to an org-level endpoint (not scoped to a project).

        Args:
            path: Path appended after the base URL.
            body: JSON-serializable request body.

        Returns:
            requests.Response object.
        """
        headers = {
            "Accept": self.JSON_ACCEPT,
            "Content-Type": "application/json",
        }
        return self._session.post(
            self._org_url(path), headers=headers, json=body
        )

    def delete(self, path: str):
        """DELETE a project-scoped endpoint.

        Args:
            path: Path appended after /groups/{group_id}.

        Returns:
            requests.Response object.
        """
        headers = {"Accept": self.JSON_ACCEPT}
        return self._session.delete(self._url(path), headers=headers)

    def delete_org(self, path: str):
        """DELETE an org-level endpoint (not scoped to a project).

        Args:
            path: Path appended after the base URL.

        Returns:
            requests.Response object.
        """
        headers = {"Accept": self.JSON_ACCEPT}
        return self._session.delete(self._org_url(path), headers=headers)

    # -- Factory -----------------------------------------------------------

    def for_group(self, group_id: str) -> "AtlasAPI":
        """Create a new AtlasAPI instance scoped to a different project.

        Shares the underlying HTTP session (and connection pool) with the
        original instance for efficiency.

        Args:
            group_id: Project ID to scope the new instance to.

        Returns:
            New AtlasAPI instance.
        """
        new = AtlasAPI.__new__(AtlasAPI)
        new._auth = self._auth
        new._session = self._session  # share the session (connection pool)
        new.group_id = group_id
        return new

    # -- High-level helpers ------------------------------------------------

    def get_org_id(self) -> str:
        """Return the organization ID that owns the current project.

        Makes a GET /groups/{groupId} call and extracts the orgId field.

        Returns:
            Organization ID string.
        """
        resp = self.get("")  # GET /groups/{groupId}
        resp.raise_for_status()
        return resp.json()["orgId"]

    def create_project(self, name: str, org_id: str) -> dict:
        """Create a new Atlas project.

        If a project with the same name already exists (HTTP 409), the
        existing project is returned instead.

        Args:
            name: Desired project name.
            org_id: Organization ID to create the project in.

        Returns:
            Project document dict.
        """
        resp = self.post_org("/groups", {"name": name, "orgId": org_id})
        if resp.status_code == 409:
            # Already exists -- find it
            all_projects = self.get_org("/groups").json().get("results", [])
            for p in all_projects:
                if p["name"] == name:
                    return p
            resp.raise_for_status()  # should not reach here
        resp.raise_for_status()
        return resp.json()

    def delete_project(self, group_id: str) -> None:
        """Delete a project by its ID.  Silently ignores 404.

        Args:
            group_id: Project ID to delete.
        """
        resp = self.delete_org(f"/groups/{group_id}")
        if resp.status_code not in (200, 202, 204, 404):
            print(
                f"  [warn] Could not delete project {group_id}: "
                f"{resp.status_code} {resp.text[:200]}"
            )

    def list_clusters(self) -> list[dict]:
        """List all clusters in the current project.

        Returns:
            List of cluster document dicts.
        """
        resp = self.get("/clusters")
        resp.raise_for_status()
        return resp.json().get("results", [])

    def get_cluster(self, name: str) -> dict:
        """Get a single cluster by name.

        Args:
            name: Cluster name.

        Returns:
            Cluster document dict.
        """
        resp = self.get(f"/clusters/{name}")
        resp.raise_for_status()
        return resp.json()

    def list_processes(self) -> list[dict]:
        """List all MongoDB processes in the current project.

        Returns:
            List of process document dicts with hostname, port, etc.
        """
        resp = self.get("/processes")
        resp.raise_for_status()
        return resp.json().get("results", [])

    def create_temp_user(self, password: str) -> dict:
        """Create a temporary read-only database user for compatibility scanning.

        Grants ``readAnyDatabase`` + ``clusterMonitor`` roles -- the minimum
        needed to run all scan checks without modifying data.

        If the user already exists (HTTP 409 from a previous interrupted run),
        it is deleted and recreated with the new password.

        Args:
            password: Password for the temporary user.

        Returns:
            Database user document dict.
        """
        body = {
            "databaseName": "admin",
            "username": TEMP_USER,
            "password": password,
            "roles": [
                {"databaseName": "admin", "roleName": "readAnyDatabase"},
                {"databaseName": "admin", "roleName": "clusterMonitor"},
            ],
        }
        resp = self.post("/databaseUsers", body)
        if resp.status_code == 409:
            # User already exists -- delete and recreate
            print("  Temp user already exists, recreating...")
            self.delete_temp_user()
            time.sleep(2)
            resp = self.post("/databaseUsers", body)
        resp.raise_for_status()
        return resp.json()

    def delete_temp_user(self) -> None:
        """Delete the temporary scan user.  Silently ignores 404 (already gone)."""
        resp = self.delete(f"/databaseUsers/admin/{TEMP_USER}")
        if resp.status_code not in (200, 202, 204, 404):
            print(
                f"  [warn] Could not delete temp user: "
                f"{resp.status_code} {resp.text[:200]}"
            )
