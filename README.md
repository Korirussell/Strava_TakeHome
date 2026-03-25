# Strava Index Analyzer

Parses Elasticsearch index data and prints three reports:
1. Top 5 indexes by size (GB)
2. Top 5 indexes by shard count
3. Top 5 under-sharded indexes with recommended shard counts

## Requirements

Python 3.10+ and the `requests` library:

```
pip install requests
```

## Usage

**Local file:**
```
python3 template.py --debug
```

**Live API server:**
```
python3 template.py --endpoint <host>
```

By default, the live mode queries the last 7 days. Override with `--days`:
```
python3 template.py --endpoint <host> --days 14
```
