import argparse
import re
import redis
import sys

from datetime import datetime, timedelta

def q1(items):
    '''Example of usage: python problem1b.py -q q1 2022-09-03:16 2022-09-04:02 2022-09-04:19 2022-09-05:01 2022-09-06:08'''

    print("Query 1")

    for item in items:
        print(f"{item} = {redis_client.hlen(item)}")

def q2(items):
    '''Example of usage: python problem1b.py -q q2 2022-09-03:16:http://example.com/?url=065 2022-09-04:02:http://example.com/?url=110 2022-09-04:19:http://example.com/?url=100 2022-09-05:01:http://example.com/?url=018 2022-09-06:08:http://example.com/?url=013'''

    print("Query 2")

    for item in items:
        map_name = convert_input(item, "q2")
        print(f"{item} = {redis_client.hlen(map_name)}")

def q3(items):
    '''Example of usage: python problem1b.py -q q3 2022-09-03:16:http://example.com/?url=065 2022-09-04:02:http://example.com/?url=110 2022-09-04:19:http://example.com/?url=100 2022-09-05:01:http://example.com/?url=018 2022-09-06:08:http://example.com/?url=013'''

    print("Query 3")

    for item in items:
        map_name = convert_input(item, "q3")
        r = redis_client.hget(map_name, 'clicks')
        if r == None:
            r = '0'
        print(f"{item} = {int(r)}")

def q4(items):
    '''Example of usage: python problem1b.py -q q4 2022-09-03:16 2022-09-04:02'''

    print("Query 4")
    if len(items) != 2:
        print("Usage: python problem1b.py -q q4 <t1> <t2>")
        sys.exit(1)

    t1 = datetime.strptime(items[0], '%Y-%m-%d:%H')
    t2 = datetime.strptime(items[1], '%Y-%m-%d:%H')

    while t1 <= t2:
        hour = t1.strftime('%Y-%m-%d:%H')
        for country in redis_client.smembers('COUNTRY-' + hour):
            country = country.decode("utf-8")
            map_name = 'COUNTRY-' + hour + '-' + country
            print(f"{hour}, {country}, {redis_client.hlen(map_name)}")

        t1 += timedelta(hours=1)


def convert_input(input, middle):
    '''Converts an input like 2022-09-03:16:http://example.com/?url=065 into 2022-09-03:16|q2|http://example.com/?url=065'''
    fields = re.split(r"(\d\d\d\d-\d\d?-\d\d?:\d\d?):(.*)", input)
    return f"{fields[1]}|{middle}|{fields[2]}"

def parse_arguments():
    parser = argparse.ArgumentParser(prog="problem1b", description="Implements Queries 1-3 programatically")

    parser.add_argument('--redis_url', '-ru', required=False, default="redis://localhost:6379",
                        help="Redis end point url; Eg: redis://localhost:6379")
    parser.add_argument('--query', '-q', required=True, choices=['q1', 'q2', 'q3', 'q4'],
                        help="Which query to run")
    parser.add_argument("items", nargs=argparse.REMAINDER, default=[])

    parsed_args = parser.parse_args()
    return parsed_args

def connect_redis(redis_url):
    global redis_client
    redis_client = redis.Redis.from_url(redis_url)

def main():
    parsed_args = parse_arguments()
    connect_redis(parsed_args.redis_url)

    if parsed_args.query == "q1":
        q1(parsed_args.items)
    elif parsed_args.query == "q2":
        q2(parsed_args.items)
    elif parsed_args.query == "q3":
        q3(parsed_args.items)
    elif parsed_args.query == "q4":
        q4(parsed_args.items)


if __name__ == '__main__':
    main()

