import json
import time
import aiohttp
import asyncio
from termcolor import colored
from dataclasses import dataclass, asdict
from typing import Dict, List

API_URL = "https://dummyjson.com/c/e8ec-a568-4318-bb34"  # dummy
RAW_FILE = "raw.json"
OUTPUT_FILE = "counts.json"
COUNTER_API_URL_TEMPLATE = "https://arn.goatcounter.com/counter/{pathname}.json"


PathnameKey = str
RedirectPath = str
Count = int
TotalCount = int
Timestamp = str
PathRedirectMapping = Dict[PathnameKey, List[RedirectPath]]
PathRedirectMappings = List[PathRedirectMapping]


@dataclass
class ApiData:
    timestamp: Timestamp
    stats: PathRedirectMappings


@dataclass
class PathCounts:
    paths_counts: Dict[RedirectPath, Count]
    total_count: TotalCount = 0


@dataclass
class TransformedData:
    timestamp: Timestamp
    stats: List[Dict[PathnameKey, PathCounts]]


def get_current_timestamp() -> Timestamp:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def transform_data_structure(
    api_data: PathRedirectMappings, timestamp: Timestamp
) -> TransformedData:
    stats: List[Dict[PathnameKey, PathCounts]] = []
    for item in api_data:
        for key, paths in item.items():
            path_counts = {path: 0 for path in paths}
            stats.append({key: PathCounts(paths_counts=path_counts)})
    return TransformedData(timestamp=timestamp, stats=stats)


async def fetch_redirect_path_count(
    session: aiohttp.ClientSession, path: RedirectPath
) -> Count:
    try:
        async with session.get(
            COUNTER_API_URL_TEMPLATE.format(pathname=path)
        ) as response:
            if response.status == 200:
                return int((await response.json()).get("count", 0))
            return 0
    except Exception:
        return 0


async def fetch_and_cache_data(session: aiohttp.ClientSession) -> ApiData:
    async with session.get(API_URL) as response:
        response_json = await response.json()
        raw_file_timestamp = get_current_timestamp()
        data = ApiData(timestamp=raw_file_timestamp, stats=response_json)

        with open(RAW_FILE, "w") as file:
            json.dump(
                {"timestamp": data.timestamp, "stats": data.stats}, file, indent=2
            )

        print(colored(f"Data fetched from the URL and saved to {RAW_FILE}.", "green"))
        return data


async def process_transformed_data(
    session: aiohttp.ClientSession, transformed_data: TransformedData
) -> None:
    for item in transformed_data.stats:
        for value in item.values():
            total_count = 0
            for path in value.paths_counts:
                count = await fetch_redirect_path_count(session, path)
                value.paths_counts[path] = count
                total_count += count
            value.total_count = total_count


async def save_transformed_data(transformed_data: TransformedData) -> None:
    out = asdict(transformed_data)

    out["stats"] = [
        {
            key: {
                "paths_counts": path_counts.paths_counts,
                "total_count": path_counts.total_count,
            }
        }
        for stat in transformed_data.stats
        for key, path_counts in stat.items()
    ]

    # Save to file
    with open(OUTPUT_FILE, "w") as file:
        json.dump(out, file, indent=2)

    print(colored(f"Transformed data saved to {OUTPUT_FILE}.", "green"))


# Main function
async def main() -> None:
    async with aiohttp.ClientSession() as session:
        api_data = await fetch_and_cache_data(session)
        transformed_data = transform_data_structure(
            api_data.stats, get_current_timestamp()
        )
        await process_transformed_data(session, transformed_data)
        await save_transformed_data(transformed_data)


if __name__ == "__main__":
    asyncio.run(main())
