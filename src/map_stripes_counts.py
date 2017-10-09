#! /usr/bin/env python
import sys

for line in sys.stdin:
    fields = line.split(",")
    try:
        user_id = int(fields[0])
        movie_id = int(fields[1])
        print('{}\t{}'.format(user_id, movie_id))
    except ValueError:
        continue
