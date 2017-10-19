{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_pairs2 -file ~/hw2/src/map_pair_counts.py -mapper ~/hw2/src/map_pair_counts.py -file ~/hw2/src/reduce_pair_users.py -reducer ~/hw2/src/reduce_pair_users.py -file ~/hw2/src/reduce_pair_cooccurrences.py -reducer ~/hw2/src/reduce_pair_cooccurrences.py >hadoop_output.text 2>&1; } 2>> hadoop_pairs_time.txt

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_pairs2 -file ~/hw2/src/map_pair_counts.py -mapper ~/hw2/src/map_pair_counts.py -file ~/hw2/src/reduce_pair_users.py -reducer ~/hw2/src/reduce_pair_users.py -file ~/hw2/src/reduce_pair_cooccurrences.py -reducer ~/hw2/src/reduce_pair_cooccurrences.py


#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings.csv -output output -file ~/hw2/src/map_pair_counts.py -mapper ~/hw2/src/map_pair_counts.py file ~/hw2/src/reducer_pairs_users.py -reducer ~/hw2/src/reducer_pairs_cooccurrences.py

#{ time hadoop jar  /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/2017022.txt -output max_min_output2 -file ~/src/map_max_min.py -mapper ~/src/map_max_min.py -file  ~/src/reducer_max_min.py -reducer ~/src/reducer_max_min.py >hadoop_output.txt 2>&1; } 2>> time.txt
