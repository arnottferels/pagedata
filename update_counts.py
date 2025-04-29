import json
import aiohttp
import asyncio
from dataclasses import dataclass, asdict
from typing import Dict, List

FETCH_URL = "https://arnottferels.github.io/a/data/redirect.json"
COUNTER_URL = "https://arn.goatcounter.com/counter/{pathname}.json"

RAW_FILE = "raw.json"
OUTPUT_FILE = "counts.json"

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
    fetch_data: PathRedirectMappings,
) -> Dict[PathnameKey, PathCounts]:
    return {
        key: PathCounts(paths_counts={path: 0 for path in paths})
        for key, paths in fetch_data.items()
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


async def fetch_and_cache_data(session: aiohttp.ClientSession) -> PathRedirectMappings:
    async with session.get(FETCH_URL) as response:
        data = await response.json()
        with open(RAW_FILE, "w") as file:
            json.dump(data, file, indent=2)
        print(f"Data fetched and saved to {RAW_FILE}.")
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
    with open(OUTPUT_FILE, "w") as file:
        json.dump(
            {
                key: {
                    "paths_counts": asdict(path_counts)["paths_counts"],
                    "total_count": asdict(path_counts)["total_count"],
                }
                for key, path_counts in transformed_data.items()
            },
            file,
            indent=2,
        )
    print(f"Transformed data saved to {OUTPUT_FILE}.")


async def main() -> None:
    async with aiohttp.ClientSession() as session:
        fetch_data = await fetch_and_cache_data(session)
        transformed_data = transform_data_structure(fetch_data)
        await process_transformed_data(session, transformed_data)
        await save_transformed_data(transformed_data)


if __name__ == "__main__":
    asyncio.run(main())
