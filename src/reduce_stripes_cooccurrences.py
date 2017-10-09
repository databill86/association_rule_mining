#! /usr/bin/env python
import sys
import ast
from collections import defaultdict
import heapq


cooccurrence_sum = 0
current_id = None
last_id = None
stripes_dict = defaultdict(lambda: defaultdict(int))

def increment_stripes(s_dict, i_id, new_stripe):
    for tup in new_stripe:

        s_dict[i_id] 



# expecting input as id_i \t [(id_j1, n1), (id_j2, n2)]
for line in sys.stdin:
   i_id, stripe = line.split('\t', 1)
   try:
       current_id = int(i_id)
       stripe = ast.literal_eval(stripe)
       
   except ValueError:
       continue
   
   # not first item and its a new pair
   # new boundary detected
   # emit previous pair and restart sum
   if current_id and (current_id != last_id):
       print(last_pair, cooccurrence_sum)
       top_20_dict[last_pair] = cooccurrence_sum
       cooccurrence_sum = 0
       for count in counts:
          cooccurrence_sum += int(count)
    # 1st pair or same pair as before
   else:
       for count in counts:
           cooccurrence_sum += int(count)
    # update working pair
   last_pair = current_pair
