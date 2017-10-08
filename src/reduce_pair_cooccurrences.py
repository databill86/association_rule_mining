#! /usr/bin/env python
import sys
import ast
import heapq

top_20_dict = {}
cooccurrence_sum = 0
current_pair = None
last_pair = None

# expecting input as (id_i, id_j) \t [1]
for line in sys.stdin:
   pair, counts = line.split('\t', 1)

   try:
      current_pair = ast.literal_eval(pair)
      # id_i, id_j = current_pair
      counts = ast.literal_eval(counts)
   except ValueError:
      continue

   # not first item and its a new pair
   # new boundary detected
   # emit previous pair and restart sum
   if current_pair and (current_pair != last_pair):
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
