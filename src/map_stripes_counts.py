#! /usr/bin/env python
import sys
from collections import defaultdict

user_dict = defaultdict(list)

for line in sys.stdin:
    fields = line.split(",")
    try:
        user_id = int(fields[0])
        movie_id = int(fields[1])
        user_dict[user_id].append(movie_id)

    except ValueError:
        continue

for user_id, movie_list in user_dict.items():
    print('{}\t{}'.format(user_id, movie_list))
