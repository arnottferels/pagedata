import aiohttp
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from dotenv import load_dotenv
from typing import Dict, List

load_dotenv()
API_URL_KEY = "COUNTS_WEBAPP_URL"
COUNTER_URL = os.getenv(API_URL_KEY) or sys.exit(f"Error: {API_URL_KEY} is missing.")

FETCH_URL = "https://arnottferels.github.io/a/json/redirect.json"


COUNTS_REDIR_MAP_FILE = "c_rm.json"
COUNTS_OUTPUT_FILE = "c.json"

PathnameKey = str
RedirectPath = str
IntCount = int
TotalCount = int

PathRedirectMappings = Dict[PathnameKey, List[RedirectPath]]


@dataclass
class PathCounts:
    paths_counts: Dict[RedirectPath, IntCount]
    total_count: TotalCount = 0


def transform_data_structure(
    rm: PathRedirectMappings,
) -> Dict[PathnameKey, PathCounts]:
    return {
        key: PathCounts(paths_counts={path: 0 for path in paths})
        for key, paths in rm.items()
    }


async def fetch_all_counts(session: aiohttp.ClientSession) -> Dict[str, int]:
    try:
        async with session.get(COUNTER_URL) as response:
            if response.status == 200:
                data = await response.json()
                return {k: int(v) for k, v in data.items()}
            else:
                print(f"Failed to fetch counts: HTTP {response.status}")
    except Exception as e:
        print(f"Exception fetching counts: {e}")
    return {}


async def fetch_and_cache_redirect_map(
    session: aiohttp.ClientSession,
) -> PathRedirectMappings:
    async with session.get(FETCH_URL) as response:
        data = await response.json()
        with open(COUNTS_REDIR_MAP_FILE, "w") as file:
            json.dump(data, file, indent=2)
        print(f"Redirect map fetched and saved to {COUNTS_REDIR_MAP_FILE}.")
        return data


async def process_transformed_data(
    all_counts: Dict[str, int],
    transformed_data: Dict[PathnameKey, PathCounts],
) -> None:
    for path_counts in transformed_data.values():
        # update counts
        path_counts.paths_counts = {
            p: all_counts.get(p, 0) for p in path_counts.paths_counts
        }
        # update total count as sum of counts
        path_counts.total_count = sum(path_counts.paths_counts.values())


async def save_transformed_data(
    transformed_data: Dict[PathnameKey, PathCounts],
) -> None:
    filtered: Dict[str, Dict[str, int | Dict[str, int]]] = {
        key: {
            "paths_counts": {p: c for p, c in pc.paths_counts.items() if c > 0},
            "total_count": pc.total_count,
        }
        for key, pc in transformed_data.items()
        if any(c > 0 for c in pc.paths_counts.values())
    }

    with open(COUNTS_OUTPUT_FILE, "w") as file:
        json.dump(filtered, file, indent=2)
    print(f"Filtered data saved to {COUNTS_OUTPUT_FILE}.")


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        redirect_map = await fetch_and_cache_redirect_map(session)
        transformed_data = transform_data_structure(redirect_map)
        all_counts = await fetch_all_counts(session)
        await process_transformed_data(all_counts, transformed_data)
        await save_transformed_data(transformed_data)


if __name__ == "__main__":
    asyncio.run(main())
