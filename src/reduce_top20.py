#! /usr/bin/env python
import sys
import ast
import heapq

pair_dict = {}
cooccurrence_sum = 0
current_pair = None
last_pair = None

# expecting input as (id_i, id_j) \t [1]
for line in sys.stdin:
    pair, counts = line.split('\t', 1)

    try:
      pair = ast.literal_eval(pair)
      # id_i, id_j = current_pair
      count = ast.literal_eval(counts)
    except ValueError:
      continue
    pair_dict[pair] = count

top_20_pairs = heapq.nlargest(20, pair_dict, key=pair_dict.__getitem__)
for p in top_20_pairs:
    print("{}\t{}".format(p, pair_dict[p]))
