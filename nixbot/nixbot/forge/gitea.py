"""Gitea client: personal-token auth, discovery via
/api/v1/user/repos with topics fetched per repo."""

from __future__ import annotations

from typing import Any

from .base import DiscoveredRepo, TokenForgeClient, check_response


class GiteaClient(TokenForgeClient):
    forge_name = "Gitea"

    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"token {self.token}"}

    async def paginated(self, url: str) -> list[dict[str, Any]]:
        # Gitea does not emit RFC 5988 Link headers reliably; walk
        # ?page=N until an empty page.
        results: list[dict[str, Any]] = []
        page = 1
        while True:
            response = await self.http.get(
                f"{url}&page={page}", headers=self.auth_headers()
            )
            check_response(response, self.forge_name)
            data = response.json()
            if not data:
                return results
            results.extend(data)
            page += 1

    async def discover_repos(
        self, *, fetch_topics: bool = False
    ) -> list[DiscoveredRepo]:
        """List repos. Topics cost one extra request per repo and are
        only needed by the one-shot legacy topic import, so they are
        skipped unless `fetch_topics` is set."""
        repos = []
        for repo in await self.paginated(
            f"{self.instance_url}/api/v1/user/repos?limit=100"
        ):
            topics: list[str] = []
            if fetch_topics:
                topics_response = await self.http.get(
                    f"{self.instance_url}/api/v1/repos/"
                    f"{repo['owner']['login']}/{repo['name']}/topics",
                    headers=self.auth_headers(),
                )
                if topics_response.status_code < 400:  # noqa: PLR2004
                    # Gitea reports repos without topics as null.
                    topics = topics_response.json().get("topics") or []
            repos.append(
                DiscoveredRepo(
                    forge="gitea",
                    forge_repo_id=str(repo["id"]),
                    owner=repo["owner"]["login"],
                    repo=repo["name"],
                    default_branch=repo.get("default_branch") or "main",
                    clone_url=repo["clone_url"],
                    private=repo.get("private", False),
                    topics=tuple(topics),
                )
            )
        return repos
