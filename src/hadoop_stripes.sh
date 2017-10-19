{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_stripes_8 -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripes_counts.py -file ~/hw2/src/reduce_stripes_users.py -reducer ~/hw2/src/reduce_stripes_users.py -file ~/hw2/src/reduce_stripes_cooccurrences.py -reducer ~/hw2/src/reduce_stripes_cooccurrences.py >stripe_output.txt 2>&1; } 2>> time.txt

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_stripes_7 -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripes_counts.py -file ~/hw2/src/reduce_stripes_users.py -reducer ~/hw2/src/reduce_stripes_users.py -file ~/hw2/src/reduce_stripes_cooccurrences.py -reducer ~/hw2/src/reduce_stripes_cooccurrences.py

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings.csv -output output -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripe_counts.py file ~/hw2/src/reducer_stripess_users.py -reducer ~/hw2/src/reducer_pairs_cooccurrences.py

