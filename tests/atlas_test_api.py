"""Test-only Atlas API extensions -- project create/delete operations.

These methods are intentionally kept OUT of the production atlas_api.py
because they can create and DELETE entire Atlas projects.  They are only
needed by the test harness (setup_test_env.py).
"""

from atlas_api import AtlasOrgAPI


class AtlasTestOrgAPI(AtlasOrgAPI):
    """AtlasOrgAPI with project lifecycle methods for testing."""

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
