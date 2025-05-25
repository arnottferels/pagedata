import aiohttp
import asyncio
import json
from dataclasses import dataclass
from typing import Dict, List

FETCH_URL = "https://arnottferels.github.io/a/data/redirect.json"
COUNTER_URL = "https://arn.goatcounter.com/counter/{pathname}.json"

COUNTS_REDIR_MAP_FILE = "c_rm.v1.json"
COUNTS_OUTPUT_FILE = "c.v1.json"

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


async def fetch_redirect_path_count(
    session: aiohttp.ClientSession, pathname: RedirectPath
) -> IntCount:
    try:
        async with session.get(COUNTER_URL.format(pathname=pathname)) as response:
            if response.status == 200:
                data = await response.json()
                return int(data["count"])
            return 0
    except Exception:
        return 0


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
    session: aiohttp.ClientSession,
    transformed_data: Dict[PathnameKey, PathCounts],
) -> None:
    for value in transformed_data.values():
        total = 0
        for pathname in value.paths_counts:
            count = await fetch_redirect_path_count(session, pathname)
            value.paths_counts[pathname] = count
            total += count
        value.total_count = total


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
        await process_transformed_data(session, transformed_data)
        await save_transformed_data(transformed_data)


if __name__ == "__main__":
    asyncio.run(main())
