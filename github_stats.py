#!/usr/bin/python3

import asyncio
import hashlib
import json
import os
import time
from typing import Dict, List, Optional, Set, Tuple, Any, cast

import aiohttp


###############################################################################
# Cache Helper
###############################################################################


class SimpleCache:
    """
    Simple file-based cache with TTL support for API responses.
    """
    
    def __init__(self, cache_dir: str = ".github_stats_cache", ttl: int = 3600):
        """
        :param cache_dir: Directory to store cache files
        :param ttl: Time-to-live in seconds (default: 1 hour)
        """
        self.cache_dir = cache_dir
        self.ttl = ttl
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self) -> None:
        """Create cache directory if it doesn't exist"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def _get_cache_path(self, key: str) -> str:
        """Get the file path for a cache key"""
        # Use SHA-256 hash to create a safe filename
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{key_hash}.json")
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a value from cache if it exists and is not expired
        :param key: Cache key
        :return: Cached value or None
        """
        cache_path = self._get_cache_path(key)
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            
            # Check if cache is expired
            if time.time() - data.get('timestamp', 0) > self.ttl:
                os.remove(cache_path)
                return None
            
            return data.get('value')
        except (json.JSONDecodeError, IOError, KeyError):
            # If cache file is corrupted, remove it
            if os.path.exists(cache_path):
                os.remove(cache_path)
            return None
    
    def set(self, key: str, value: Any) -> None:
        """
        Store a value in cache
        :param key: Cache key
        :param value: Value to cache
        """
        cache_path = self._get_cache_path(key)
        try:
            with open(cache_path, 'w') as f:
                json.dump({
                    'timestamp': time.time(),
                    'value': value
                }, f)
        except (IOError, TypeError):
            # If caching fails, just continue without caching
            pass


###############################################################################
# Main Classes
###############################################################################


class Queries(object):
    """
    Class with functions to query the GitHub GraphQL (v4) API and the REST (v3)
    API. Also includes functions to dynamically generate GraphQL queries.
    """

    def __init__(
        self,
        username: str,
        access_token: str,
        session: aiohttp.ClientSession,
        max_connections: int = 50,
    ):
        self.username = username
        self.access_token = access_token
        self.session = session
        self.semaphore = asyncio.Semaphore(max_connections)

    async def query(self, generated_query: str) -> Dict:
        """
        Make a request to the GraphQL API using the authentication token from
        the environment
        :param generated_query: string query to be sent to the API
        :return: decoded GraphQL JSON output
        """
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        try:
            async with self.semaphore:
                r_async = await self.session.post(
                    "https://api.github.com/graphql",
                    headers=headers,
                    json={"query": generated_query},
                )
            if r_async.status == 401:
                raise RuntimeError(
                    "GitHub API returned 401 Unauthorized. "
                    "Your ACCESS_TOKEN secret is missing, expired, or lacks the required scopes. "
                    "Please generate a new token at https://github.com/settings/tokens"
                )
            if r_async.status == 403:
                raise RuntimeError(
                    "GitHub API returned 403 Forbidden. "
                    "Your token may lack required permissions or you have hit a rate limit."
                )
            result = await r_async.json()
            if result is not None:
                if "errors" in result:
                    print(f"GraphQL errors: {result['errors']}")
                return result
        except RuntimeError:
            raise
        except Exception as e:
            print(f"GraphQL query failed for user {self.username}: {e}")
        return dict()

    async def query_rest(self, path: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a request to the REST API
        :param path: API path to query
        :param params: Query parameters to be passed to the API
        :return: deserialized REST JSON output
        """

        for _ in range(60):
            headers = {
                "Authorization": f"token {self.access_token}",
            }
            if params is None:
                params = dict()
            if path.startswith("/"):
                path = path[1:]
            try:
                async with self.semaphore:
                    r_async = await self.session.get(
                        f"https://api.github.com/{path}",
                        headers=headers,
                        params=tuple(params.items()),
                    )
                if r_async.status == 401:
                    raise RuntimeError(
                        "GitHub API returned 401 Unauthorized. "
                        "Your ACCESS_TOKEN secret is missing, expired, or lacks the required scopes. "
                        "Please generate a new token at https://github.com/settings/tokens"
                    )
                if r_async.status == 202:
                    await asyncio.sleep(5)
                    continue

                result = await r_async.json()
                if result is not None:
                    return result
            except RuntimeError:
                raise
            except Exception as e:
                print(f"REST query failed for path '{path}': {e}")
                # Return empty dict to allow graceful degradation
                return dict()
        return dict()

    async def trigger_stats(self, path: str) -> None:
        """
        Fire a single request to trigger async stat computation on GitHub's side.
        Does not retry; intended as a pre-warm call.
        """
        if path.startswith("/"):
            path = path[1:]
        headers = {"Authorization": f"token {self.access_token}"}
        try:
            async with self.semaphore:
                await self.session.get(
                    f"https://api.github.com/{path}",
                    headers=headers,
                )
        except Exception:
            pass

    @staticmethod
    def repos_overview(
        contrib_cursor: Optional[str] = None, owned_cursor: Optional[str] = None
    ) -> str:
        """
        :return: GraphQL query with overview of user repositories
        """
        return f"""{{
  viewer {{
    login,
    name,
    repositories(
        first: 100,
        orderBy: {{
            field: UPDATED_AT,
            direction: DESC
        }},
        isFork: false,
        after: {"null" if owned_cursor is None else '"'+ owned_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
    repositoriesContributedTo(
        first: 100,
        includeUserRepositories: false,
        orderBy: {{
            field: UPDATED_AT,
            direction: DESC
        }},
        contributionTypes: [
            COMMIT,
            PULL_REQUEST,
            REPOSITORY,
            PULL_REQUEST_REVIEW
        ]
        after: {"null" if contrib_cursor is None else '"'+ contrib_cursor +'"'}
    ) {{
      pageInfo {{
        hasNextPage
        endCursor
      }}
      nodes {{
        nameWithOwner
        stargazers {{
          totalCount
        }}
        forkCount
        languages(first: 10, orderBy: {{field: SIZE, direction: DESC}}) {{
          edges {{
            size
            node {{
              name
              color
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

    @staticmethod
    def contrib_years() -> str:
        """
        :return: GraphQL query to get all years the user has been a contributor
        """
        return """
query {
  viewer {
    contributionsCollection {
      contributionYears
    }
  }
}
"""

    @staticmethod
    def contribs_by_year(year: str) -> str:
        """
        :param year: year to query for
        :return: portion of a GraphQL query with desired info for a given year
        """
        return f"""
    year{year}: contributionsCollection(
        from: "{year}-01-01T00:00:00Z",
        to: "{int(year) + 1}-01-01T00:00:00Z"
    ) {{
      contributionCalendar {{
        totalContributions
      }}
    }}
"""

    @classmethod
    def all_contribs(cls, years: List[str]) -> str:
        """
        :param years: list of years to get contributions for
        :return: query to retrieve contribution information for all user years
        """
        by_years = "\n".join(map(cls.contribs_by_year, years))
        return f"""
query {{
  viewer {{
    {by_years}
  }}
}}
"""


class Stats(object):
    """
    Retrieve and store statistics about GitHub usage.
    """

    def __init__(
        self,
        username: str,
        access_token: str,
        session: aiohttp.ClientSession,
        exclude_repos: Optional[Set] = None,
        exclude_langs: Optional[Set] = None,
        ignore_forked_repos: bool = False,
        enable_cache: bool = False,
        cache_ttl: int = 3600,
        include_views: bool = True,
        include_lines_changed: bool = True,
    ):
        self.username = username
        self._ignore_forked_repos = ignore_forked_repos
        self._exclude_repos = set() if exclude_repos is None else exclude_repos
        self._exclude_langs = set() if exclude_langs is None else exclude_langs
        self._include_views = include_views
        self._include_lines_changed = include_lines_changed
        self.queries = Queries(username, access_token, session)
        self.cache = SimpleCache(ttl=cache_ttl) if enable_cache else None

        self._name: Optional[str] = None
        self._stargazers: Optional[int] = None
        self._forks: Optional[int] = None
        self._total_contributions: Optional[int] = None
        self._languages: Optional[Dict[str, Any]] = None
        self._repos: Optional[Set[str]] = None
        self._lines_changed: Optional[Tuple[int, int]] = None
        self._views: Optional[int] = None

    async def to_str(self) -> str:
        """
        :return: summary of all available statistics
        """
        languages = await self.languages_proportional
        formatted_languages = "\n  - ".join(
            [f"{k}: {v:0.4f}%" for k, v in languages.items()]
        )
        lines_changed = await self.lines_changed
        return f"""Name: {await self.name}
Stargazers: {await self.stargazers:,}
Forks: {await self.forks:,}
All-time contributions: {await self.total_contributions:,}
Repositories with contributions: {len(await self.repos)}
Lines of code added: {lines_changed[0]:,}
Lines of code deleted: {lines_changed[1]:,}
Lines of code changed: {lines_changed[0] + lines_changed[1]:,}
Project page views: {await self.views:,}
Languages:
  - {formatted_languages}"""

    async def get_stats(self) -> None:
        """
        Get lots of summary statistics using one big query. Sets many attributes
        """
        self._stargazers = 0
        self._forks = 0
        self._languages = dict()
        self._repos = set()

        exclude_langs_lower = {x.lower() for x in self._exclude_langs}

        next_owned = None
        next_contrib = None
        while True:
            raw_results = await self.queries.query(
                Queries.repos_overview(
                    owned_cursor=next_owned, contrib_cursor=next_contrib
                )
            )
            raw_results = raw_results if raw_results is not None else {}

            self._name = raw_results.get("data", {}).get("viewer", {}).get("name", None)
            if self._name is None:
                self._name = (
                    raw_results.get("data", {})
                    .get("viewer", {})
                    .get("login", "No Name")
                )

            contrib_repos = (
                raw_results.get("data", {})
                .get("viewer", {})
                .get("repositoriesContributedTo", {})
            )
            owned_repos = (
                raw_results.get("data", {}).get("viewer", {}).get("repositories", {})
            )

            repos = owned_repos.get("nodes", [])
            if not self._ignore_forked_repos:
                repos += contrib_repos.get("nodes", [])

            for repo in repos:
                if repo is None:
                    continue
                name = repo.get("nameWithOwner")
                if name in self._repos or name in self._exclude_repos:
                    continue
                self._repos.add(name)
                self._stargazers += repo.get("stargazers").get("totalCount", 0)
                self._forks += repo.get("forkCount", 0)

                for lang in repo.get("languages", {}).get("edges", []):
                    name = lang.get("node", {}).get("name", "Other")
                    languages = await self.languages
                    if name.lower() in exclude_langs_lower:
                        continue
                    if name in languages:
                        languages[name]["size"] += lang.get("size", 0)
                        languages[name]["occurrences"] += 1
                    else:
                        languages[name] = {
                            "size": lang.get("size", 0),
                            "occurrences": 1,
                            "color": lang.get("node", {}).get("color"),
                        }

            if owned_repos.get("pageInfo", {}).get(
                "hasNextPage", False
            ) or contrib_repos.get("pageInfo", {}).get("hasNextPage", False):
                next_owned = owned_repos.get("pageInfo", {}).get(
                    "endCursor", next_owned
                )
                next_contrib = contrib_repos.get("pageInfo", {}).get(
                    "endCursor", next_contrib
                )
            else:
                break

        # TODO: Improve languages to scale by number of contributions to
        #       specific filetypes
        langs_total = sum([v.get("size", 0) for v in self._languages.values()])
        for k, v in self._languages.items():
            v["prop"] = 100 * (v.get("size", 0) / langs_total)

    @property
    async def name(self) -> str:
        """
        :return: GitHub user's name (e.g., Jacob Strieb)
        """
        if self._name is not None:
            return self._name
        await self.get_stats()
        assert self._name is not None
        return self._name

    @property
    async def stargazers(self) -> int:
        """
        :return: total number of stargazers on user's repos
        """
        if self._stargazers is not None:
            return self._stargazers
        await self.get_stats()
        assert self._stargazers is not None
        return self._stargazers

    @property
    async def forks(self) -> int:
        """
        :return: total number of forks on user's repos
        """
        if self._forks is not None:
            return self._forks
        await self.get_stats()
        assert self._forks is not None
        return self._forks

    @property
    async def languages(self) -> Dict:
        """
        :return: summary of languages used by the user
        """
        if self._languages is not None:
            return self._languages
        await self.get_stats()
        assert self._languages is not None
        return self._languages

    @property
    async def languages_proportional(self) -> Dict:
        """
        :return: summary of languages used by the user, with proportional usage
        """
        if self._languages is None:
            await self.get_stats()
            assert self._languages is not None

        return {k: v.get("prop", 0) for (k, v) in self._languages.items()}

    @property
    async def repos(self) -> Set[str]:
        """
        :return: list of names of user's repos
        """
        if self._repos is not None:
            return self._repos
        await self.get_stats()
        assert self._repos is not None
        return self._repos

    @property
    async def total_contributions(self) -> int:
        """
        :return: count of user's total contributions as defined by GitHub
        """
        if self._total_contributions is not None:
            return self._total_contributions

        # Check cache first
        cache_key = f"total_contributions_{self.username}"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                self._total_contributions = cached
                return self._total_contributions

        self._total_contributions = 0
        years = (
            (await self.queries.query(Queries.contrib_years()))
            .get("data", {})
            .get("viewer", {})
            .get("contributionsCollection", {})
            .get("contributionYears", [])
        )
        by_year = (
            (await self.queries.query(Queries.all_contribs(years)))
            .get("data", {})
            .get("viewer", {})
            .values()
        )
        for year in by_year:
            self._total_contributions += year.get("contributionCalendar", {}).get(
                "totalContributions", 0
            )
        
        # Store in cache
        if self.cache:
            self.cache.set(cache_key, self._total_contributions)
        
        return cast(int, self._total_contributions)

    @property
    async def lines_changed(self) -> Tuple[int, int]:
        """
        :return: count of total lines added, removed, or modified by the user
        """
        if self._lines_changed is not None:
            return self._lines_changed
        
        # Return zeros if this stat is disabled
        if not self._include_lines_changed:
            self._lines_changed = (0, 0)
            return self._lines_changed
        
        # Check cache first
        cache_key = f"lines_changed_{self.username}"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                self._lines_changed = tuple(cached)
                return self._lines_changed
        
        repos = await self.repos

        # Pre-warm: trigger GitHub to compute stats for all repos in parallel,
        # then wait for computation to finish before fetching results.
        await asyncio.gather(
            *[self.queries.trigger_stats(f"/repos/{repo}/stats/contributors") for repo in repos],
            return_exceptions=True,
        )
        await asyncio.sleep(30)

        async def fetch_repo_stats(repo: str) -> Tuple[int, int]:
            """
            Fetch contributor stats for a single repo.
            :param repo: Repository name in format 'owner/name'
            :return: Tuple of (additions, deletions) for the user
            """
            additions = 0
            deletions = 0
            r = await self.queries.query_rest(f"/repos/{repo}/stats/contributors")
            for author_obj in r:
                # Handle malformed response from the API by skipping this repo
                if not isinstance(author_obj, dict) or not isinstance(
                    author_obj.get("author", {}), dict
                ):
                    continue
                author = author_obj.get("author", {}).get("login", "")
                if author != self.username:
                    continue

                for week in author_obj.get("weeks", []):
                    additions += week.get("a", 0)
                    deletions += week.get("d", 0)
            return (additions, deletions)
        
        # Fetch stats for all repos in parallel with exception handling
        results = await asyncio.gather(*[fetch_repo_stats(repo) for repo in repos], return_exceptions=True)
        
        # Sum up all results, skipping exceptions
        additions = 0
        deletions = 0
        for r in results:
            if isinstance(r, Exception):
                print(f"Error fetching repo stats: {r}")
                continue
            additions += r[0]
            deletions += r[1]

        self._lines_changed = (additions, deletions)
        
        # Store in cache
        if self.cache:
            self.cache.set(cache_key, list(self._lines_changed))
        
        return self._lines_changed

    @property
    async def views(self) -> int:
        """
        Note: only returns views for the last 14 days (as-per GitHub API)
        :return: total number of page views the user's projects have received
        """
        if self._views is not None:
            return self._views

        # Return zero if this stat is disabled
        if not self._include_views:
            self._views = 0
            return self._views
        
        # Check cache first
        cache_key = f"views_{self.username}"
        if self.cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                self._views = cached
                return self._views

        repos = await self.repos
        
        async def fetch_repo_views(repo: str) -> int:
            """
            Fetch traffic views for a single repo.
            :param repo: Repository name in format 'owner/name'
            :return: Total view count for the last 14 days
            """
            count = 0
            r = await self.queries.query_rest(f"/repos/{repo}/traffic/views")
            for view in r.get("views", []):
                count += view.get("count", 0)
            return count
        
        # Fetch views for all repos in parallel with exception handling
        results = await asyncio.gather(*[fetch_repo_views(repo) for repo in repos], return_exceptions=True)
        
        # Sum up all results, skipping exceptions
        total = 0
        for r in results:
            if isinstance(r, Exception):
                print(f"Error fetching repo views: {r}")
                continue
            total += r

        self._views = total
        
        # Store in cache
        if self.cache:
            self.cache.set(cache_key, self._views)
        
        return total


###############################################################################
# Main Function
###############################################################################


async def main() -> None:
    """
    Used mostly for testing; this module is not usually run standalone
    """
    access_token = os.getenv("ACCESS_TOKEN")
    user = os.getenv("GITHUB_ACTOR")
    if access_token is None or user is None:
        raise RuntimeError(
            "ACCESS_TOKEN and GITHUB_ACTOR environment variables cannot be None!"
        )
    async with aiohttp.ClientSession() as session:
        s = Stats(user, access_token, session)
        print(await s.to_str())


if __name__ == "__main__":
    asyncio.run(main())
