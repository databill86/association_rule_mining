#!/bin/bash
# run hadoop stripes process or time it

hadoop_input_fn=input/ratings_processed.csv
hadoop_output_fn=output/output_stripes1
bash_output_fn=stripes_hp_output.txt
bash_time_output=stripes_hp_time.txt

echo "#" $hadoop_input_fn >> $bash_time_output

{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input $hadoop_input_fn -output $hadoop_output_fn -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripes_counts.py -file ~/hw2/src/reduce_stripes_users.py -reducer ~/hw2/src/reduce_stripes_users.py -file ~/hw2/src/reduce_stripes_cooccurrences.py -reducer ~/hw2/src/reduce_stripes_cooccurrences.py >$bash_output_fn 2>&1; } 2>> $bash_time_output

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_stripes_7 -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripes_counts.py -file ~/hw2/src/reduce_stripes_users.py -reducer ~/hw2/src/reduce_stripes_users.py -file ~/hw2/src/reduce_stripes_cooccurrences.py -reducer ~/hw2/src/reduce_stripes_cooccurrences.py

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings.csv -output output -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripe_counts.py file ~/hw2/src/reducer_stripess_users.py -reducer ~/hw2/src/reducer_pairs_cooccurrences.py

