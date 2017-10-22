#!/bin/bash
# run hadoop stripes process or time it

input_fn=sixteenth_file.csv
hadoop_input_fn="input/$input_fn"

hadoop_output_fn=output_pairs/sixteenthagain
bash_output_fn=pairs_hp_output.txt
bash_time_output=pairs_hp_time.txt

#wc
local_path="/home/sarmstr5/hw2/data/truncated_files/$input_fn"
file_wc=$(cat $local_path | wc -l)

echo "#" $hadoop_input_fn >> $bash_time_output
echo "#" $file_wc >> $bash_time_output

{ time hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input $hadoop_input_fn -output $hadoop_output_fn -file ~/hw2/src/hadoop_code/map_stripes_counts.py -mapper ~/hw2/src/hadoop_code/map_stripes_counts.py -file ~/hw2/src/hadoop_code/reduce_stripes_users.py -reducer ~/hw2/src/hadoop_code/reduce_stripes_users.py -file ~/hw2/src/hadoop_code/reduce_stripes_cooccurrences.py -reducer ~/hw2/src/hadoop_code/reduce_stripes_cooccurrences.py >$bash_output_fn 2>&1; } 2>> $bash_time_output

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings_processed.csv -output output_stripes_7 -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripes_counts.py -file ~/hw2/src/reduce_stripes_users.py -reducer ~/hw2/src/reduce_stripes_users.py -file ~/hw2/src/reduce_stripes_cooccurrences.py -reducer ~/hw2/src/reduce_stripes_cooccurrences.py

#hadoop jar /apps/hadoop-2.4.1/share/hadoop/tools/lib/hadoop-streaming-2.4.1.jar -input input/ratings.csv -output output -file ~/hw2/src/map_stripes_counts.py -mapper ~/hw2/src/map_stripe_counts.py file ~/hw2/src/reducer_stripess_users.py -reducer ~/hw2/src/reducer_pairs_cooccurrences.py


#   28690615 2017-10-20 22:50 input/eighth_file.csv
#   72917320 2017-10-21 20:17 input/five_eighths.csv
#   65544659 2017-10-21 20:18 input/nine_sixs.csv
#   58173094 2017-10-21 20:20 input/one_half.csv
#   58201273 2017-10-20 22:50 input/quarter_file.csv
#  119918080 2017-10-13 13:32 input/ratings_processed.csv
#     552258 2017-10-19 12:43 input/ratings_small_processed.csv
#   13933890 2017-10-20 22:50 input/sixteenth_file.csv
#   87929620 2017-10-21 20:19 input/three_quarters.csv
