#!/bin/bash
# run hadoop pairs process or time it

input_fn=five_eighths.csv

# input output dirs
hadoop_input_fn="input/$input_fn"
hadoop_output_fn="pairs_output/$input_fn"
bast_output_fn=pairs_hp_output.txt
bash_time_output=pairs_hp_time.txt

# python files
mapper_fn=~/hw2/src/hadoop_code/map_pair_counts.py
reducer_users_fn=~/hw2/src/hadoop_code/reduce_pair_users.py
reducer_occurrences_fn=~/hw2/src/hadoop_code/reduce_pair_cooccurrences.py

#wc
local_path="/home/sarmstr5/hw2/data/truncated_files/$input_fn"
file_wc=$(cat $local_path | wc -l)

echo "#" $file_wc >> $bash_time_output

{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input $hadoop_input_fn -output $hadoop_output_fn -file $mapper_fn  -mapper $mapper_fn -file $reducer_users_fn -reducer $reducer_users_fn -file $reducer_occurrences_fn -reducer $reducer_occurrences_fn >$bash_output_fn 2>&1; } 2>> $bash_time_output
echo "#" $hadoop_output_fn >> $bash_output_fn
