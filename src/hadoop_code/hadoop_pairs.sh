#!/bin/bash
# run hadoop pairs process or time it

# input output dirs
hadoop_input_fn=input/ratings_processed.csv
hadoop_output_fn=output_pairs_all
bast_output_fn=pairs_hp_output.txt
bash_time_output=pairs_hp_time.txt

# python files
mapper_fn=~/hw2/src/map_pair_counts.py
reducer_users_fn=~/hw2/src/reduce_pair_users.py
reducer_occurrences_fn=~/hw2/src/reduce_pair_cooccurrences.py

echo "#" $hadoop_input_fn >> $bash_time_output

{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input $hadoop_input_fn -output $hadoop_output_fn -file $mapper_fn  -mapper $mapper_fn -file $reducer_users_fn -reducer $reducer_users_fn -file $reducer_occurrences_fn -reducer $reducer_occurrences_fn >$bash_output_fn 2>&1; } 2>> $bash_time_output
