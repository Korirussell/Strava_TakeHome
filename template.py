import argparse
import sys
import math
import json
import requests
from datetime import date, timedelta


'''
Go through index data from local json file or live elascicserach.
Produce three reports:
1. Top 5 indexes by size (GB)
2. Top 5 indexes by shard count
3. Top 5 shard offendors : indexes with the largest difference between the largest and smallest shard size (GB)
'''

#Constants
BYTES_PER_GB = 1_073_741_824
SHARD_TARGET_GB = 30
TOP_N = 5
REQUEST_TIMEOUT = 10

#Data ingestion
def get_data_from_file(filepath):
    # Read index data from local json file -> returns list of index data
    with open(filepath, "r") as f:
        return json.load(f)

#Data ingestion for live elasticsearch endpoint
def build_daily_urls(endpoint, days):
    # build one url per day for the last N days -> returns list of urls
    urls = []
    for i in range(1, days+1):
        target = date.today() - timedelta(days=i)
        year = target.strftime("%Y")
        month = target.strftime("%m")
        day = target.strftime("%d")
        # year month day wildcard matches any index containg that date regardles of prefix 
        url = (
            f"https://{endpoint}/_cat/indices/" 
            f"*{year}*{month}*{day}"  
            f"?v&h=index,pri.store.size,pri&format=json&bytes=b" 
        )
        urls.append(url)
    return urls

def get_data_from_server(endpoint, days):
    urls = build_daily_urls(endpoint, days)
    all_records = []
    # plain for loop to make 7 sequetial requests. No need for threading or asyncio since input size is so small.
    for url in urls:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()  # Raise an error for bad status codes
        all_records.extend(response.json())
    return all_records

# normalize + math helper functions
def bytes_to_gb(bytes):
    return int(bytes) / float(BYTES_PER_GB)

def recommended_shards(size_gb):
    # 1 shard per 30GB of data, round up to ensure we don't exceed target shard size
    return math.ceil(size_gb / SHARD_TARGET_GB)

def normalize(raw_records):
    records = []
    for raw in raw_records:
        raw_size = raw.get("pri.store.size") or "0"
        raw_pri = raw.get("pri") or "1"
        size_gb = bytes_to_gb(raw_size)
        shards = int(raw_pri)
        records.append({
            "name": raw["index"],
            "size_gb": size_gb,
            "shards": shards,
            "recommended_shards": recommended_shards(size_gb),
        })
    return records

# print largest index
def print_largest_indexes(records: list[dict]) -> None:
    top = sorted(records, key=lambda r: r["size_gb"], reverse=True)[:TOP_N]

    print("\n" + "┌" + "─"*60 + "┐") # necessary pretty borders
    print(f"│ Report 1: Top {TOP_N} Indexes by Size (GB)                       │") 
    print("└" + "─" * 60 + "┘")
    print(f" {'Rank' :<6} {'Size (GB)':>10}    Index Name")
    print(f" {'─'*7} {'─'*10}   {'─'*40}")

    for rank, r in enumerate(top, start=1):
        print(f" {rank:<6} {r['size_gb']:>10.2f}    {r['name']}")

# print most shards
def print_most_shards(records) :
    top = sorted(records, key=lambda r: r["shards"], reverse=True)[:TOP_N]

    print("\n" + "┌" + "─"*60 + "┐") # necessary pretty borders
    print(f"│ Report 2: Top {TOP_N} Indexes by Shard Count                     │") 
    print("└" + "─" * 60 + "┘")
    print(f" {'Rank' :<6} {'Shards':>8}    Index Name")
    print(f" {'─'*7} {'─'*10}   {'─'*40}")

    for rank, r in enumerate(top, start=1):
        print(f" {rank:<6} {r['shards']:>8}    {r['name']}")

#print least balanced
def print_least_balanced(records) :
    offendors = [r for r in records if r["shards"] < r["recommended_shards"]]
    top = sorted(offendors, key=lambda r: r["size_gb"] / r["shards"] if r["shards"] > 0 else float("inf"), reverse=True)[:TOP_N]

    print("\n" + "┌" + "─"*60 + "┐") # necessary pretty borders
    print(f"│ Report 3: Top {TOP_N} Under Sharded Indexes                       │") 
    print("└" + "─" * 60 + "┘")
    print(f" {'Rank' :<6} {'Current':>9}     {'Recommended':>13}    Index Name")
    print(f" {'─'*7} {'─'*10}   {'─'*40}")

    for rank, r in enumerate(top, start=1):
        print(f" {rank:<6} {r['shards']:>9}     {r['recommended_shards']:>13}    {r['name']}")



def main():
    parser = argparse.ArgumentParser(description="Process index data.")
    parser.add_argument("--endpoint", type=str, default="",
                        help="Logging endpoint")
    parser.add_argument("--debug", action="store_true",
                        help="Debug flag used to run locally")
    parser.add_argument("--days", type=int, default=7,
                        help="Number of days of data to parse")
    args = parser.parse_args()

    data = None

    if args.debug:
        try:
            data = get_data_from_file("indexes.json")
        except Exception as err:
            sys.exit("Error reading data from file. Error: " + str(err))
    else:
        try:
            data = get_data_from_server(args.endpoint, args.days)
        except Exception as err:
            sys.exit("Error reading data from API endpoint. Error: " + str(err))

    records = normalize(data) # data normalized

    print_largest_indexes(records)
    print_most_shards(records)
    print_least_balanced(records)

if __name__ == '__main__':
    main()
