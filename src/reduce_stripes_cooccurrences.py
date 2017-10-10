#! /usr/bin/env python
import sys
import ast
from collections import defaultdict
import heapq


cooccurrence_sum = 0
current_id = None
last_id = None
# dictionary of dictionaries with default value pf 0
#stripes_dict = defaultdict(lambda: defaultdict(int))
pair_dict = defaultdict(int)

def emit_stripe(pair_dict):
    for pair, count in pair_dict.items():
        print("{}\t{}".format(pair,count))

def increment_stripes(pair_dict, i_id, new_stripe):
    # stripe is a list of tups, [(id_j1, n1), (id_j2, n2), ...]
    for tup in new_stripe:
        j_id = tup[0]
        count = tup[1]
        pair = (i_id, j_id)
        pair_dict[pair] += count
    return pair_dict

# expecting input as id_i \t [(id_j1, n1), (id_j2, n2)]
for line in sys.stdin:
   i_id, stripe = line.split('\t', 1)
   try:
       current_id = int(i_id)
       stripe = ast.literal_eval(stripe)
       
   except ValueError:
       continue
   
   # new boundary detected
   # not first id and new id
   # emit previous pair and restart sum
   if current_id and (current_id != last_id):
       emit_stripe(pair_dict)
       #top_20_dict[last_pair] = cooccurrence_sum
       pair_dict = increment_stripes(defaultdict(int), current_id, stripe)

    # 1st pair or same pair as before
   else:
       pair_dict = increment_stripes(defaultdict(int), current_id, stripe)
    # update working pair
   last_id = current_id

emit_stripe(pair_dict)
