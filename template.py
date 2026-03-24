import argparse
import sys
import math
import sys
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
            f""*{year}*{month}*{day}"  
            f"?v&h=index,store.size,pri&format=json&bytes=b" 
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
    return bytes / BYTES_PER_GB

def recommended_shards(size_gb):
    # 1 shard per 30GB of data, round up to ensure we don't exceed target shard size
    return math.ceil(size_gb / SHARD_TARGET_GB)

def normalize(raw_records):
    records = []
    for raw in raw_records:
        size_gb = bytes_to_gb(raw.get("pri.store.size") or "0")
        shards = int(raw.get("pri") or "0")
        records.append({
            "name": raw["index"],
            "size_gb": size_gb,
            "shards": shards,
            "recommended_shards": recommended_shards(size_gb),
        })
    return records

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

    print_largest_indexes(data)
    print_most_shards(data)
    print_least_balanced(data)

if __name__ == '__main__':
    main()
