#!/bin/bash
# run hadoop pairs process or time it

hadoop_input_fn=input/ratings_processed.csv
hadoop_output_fn=output/output_pairs3
bast_output_fn=pairs_hp_output.txt
bash_time_output=pairs_hp_time.txt

echo "#" $hadoop_input_fn >> $bash_time_output

{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input $hadoop_input_fn -output $hadoop_output_fn -file ~/hw2/src/map_pair_counts.py -mapper ~/hw2/src/map_pair_counts.py -file ~/hw2/src/reduce_pair_users.py -reducer ~/hw2/src/reduce_pair_users.py -file ~/hw2/src/reduce_pair_cooccurrences.py -reducer ~/hw2/src/reduce_pair_cooccurrences.py >$bash_output_fn 2>&1; } 2>> $bash_time_output

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_pairs2 -file ~/hw2/src/map_pair_counts.py -mapper ~/hw2/src/map_pair_counts.py -file ~/hw2/src/reduce_pair_users.py -reducer ~/hw2/src/reduce_pair_users.py -file ~/hw2/src/reduce_pair_cooccurrences.py -reducer ~/hw2/src/reduce_pair_cooccurrences.py


#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings.csv -output output -file ~/hw2/src/map_pair_counts.py -mapper ~/hw2/src/map_pair_counts.py file ~/hw2/src/reducer_pairs_users.py -reducer ~/hw2/src/reducer_pairs_cooccurrences.py

#{ time hadoop jar  /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/2017022.txt -output max_min_output2 -file ~/src/map_max_min.py -mapper ~/src/map_max_min.py -file  ~/src/reducer_max_min.py -reducer ~/src/reducer_max_min.py >hadoop_output.txt 2>&1; } 2>> time.txt
