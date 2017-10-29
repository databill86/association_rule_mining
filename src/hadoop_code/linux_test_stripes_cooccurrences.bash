#cat ../test_input/ratings_tiny_processed.csv | python map_stripes_counts.py | sort -k1,1 | python reduce_stripes_users.py | sort -k1,1 | python reduce_stripes_cooccurrences.py
cat ../../data/ratings_processed.csv | python map_stripes_counts.py | sort -k1,1 | python reduce_stripes_users.py | sort -k1,1 | python reduce_stripes_cooccurrences.py | sort -k1,1 | head -n50 > local_output.txt
