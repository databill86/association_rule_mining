cat ../test_input/ratings_small_processed.csv | python map_pair_counts.py | sort -k1,1 | python reduce_pair_users.py
#cat ../test_input/ratings_small_processed.csv | python map_pair_counts.py | sort -k1,1 | python test_reducer.py
