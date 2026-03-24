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
