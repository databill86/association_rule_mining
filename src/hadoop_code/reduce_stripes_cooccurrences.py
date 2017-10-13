#! /usr/bin/env python
import sys
import ast
from collections import defaultdict
from collections import Counter
import heapq

current_id = None
last_id = None
# dictionary of dictionaries with default value pf 0
#stripes_dict = defaultdict(lambda: defaultdict(int))
pair_dict = defaultdict(int)

top_20 = True

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
       if not top_20:
           emit_stripe(pair_dict)
           pair_dict = defaultdict(int)
       #top_20_dict[last_pair] = cooccurrence_sum
       pair_dict = increment_stripes(pair_dict, current_id, stripe)

    # 1st pair or same pair as before
   else:
       pair_dict = increment_stripes(pair_dict, current_id, stripe)
    # update working pair
   last_id = current_id
if top_20:
    # counter is easier to work with
    #pair_dict = dict(Counter(pair_dict).most_common(20))
    # heap would be faster though especially with the full sized set
    k_keys = heapq.nlargest(20, pair_dict, key=pair_dict.get)
    pair_dict = {k: pair_dict[k] for k in k_keys}


emit_stripe(pair_dict)
