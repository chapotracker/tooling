import argparse
import json
import redis
import time
from collections import defaultdict


def extract(host: str, port: int, db: int, filename: str) -> int:
    r = redis.Redis(host=host, db=db, port=port)

    # Take advantage of how redis already batches scans to send reasonably sized MGET requests for the values
    cursor = "0"
    data = {}
    while cursor != 0:
        cursor, keys = r.scan(cursor=cursor)
        values = r.mget(*keys)
        data.update(dict(zip([k.decode() for k in keys], [v.decode() for v in values])))

    # Data is stored as:
    # subreddit|user => timestamp|link
    # We want to aggregate by user, giving us:
    # { ... user: { sub1: {"timestamp": ts, "link": link}, sub2: ...} ... }
    users = defaultdict(lambda: defaultdict(dict))
    for k, v in data.items():
        subreddit, _, username = k.partition("|")
        timestamp, _, link = v.partition("|")
        users[username][subreddit] = {"timestamp": int(timestamp), "link": link}
    
    # Now remove people who only post on one subreddit as well as AutoModerator
    users = {k: v for k, v in users.items() if len(v) > 1 and k != "AutoModerator"}

    with open(filename, "w") as fh:
        json.dump(users, fh)

    return len(users)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export data from Redis to JSON")
    parser.add_argument("-d", "--db", help="Redis DB", type=int, default=1)
    parser.add_argument("-H", "--host", help="Redis host", default="localhost")
    parser.add_argument("-o", "--output", help="JSON output file", default=f"users.{int(time.time())}.json")
    parser.add_argument("-p", "--port", help="Redis port", type=int, default=6379)
    args = parser.parse_args()

    start = time.time()
    n_users = extract(args.host, args.port, args.db, args.output)
    print(f"Extracted data for {n_users} users in {time.time() - start:.4f} seconds.")


if __name__ == "__main__":
    main()
