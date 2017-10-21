import findspark
findspark.init()

from pyspark import SparkContext
from pyspark import SparkConf
import itertools
from collections import defaultdict
import os

def save_default_dict(dict_file, fn):
    with open(fn, 'wb') as output:
        w = csv.writer(output)
        w.writerow(["m1_m2","counts"])
        for k,v in dict_file.iteritems():
            w.writerow([k,v])

def get_abs_file_path(file_dir, fn):
    cur_dir = os.path.abspath(os.curdir)
    return os.path.normpath(os.path.join(cur_dir, "..", file_dir, fn))

def main():
    # initialize spark
    conf = SparkConf().setMaster("local").setAppName("spark_cooccurrences.py")
    sc = SparkContext(conf = conf)

    # read in file
    input_dir = "test_input"
    input_fn = "ratings_tiny_processed.csv"
    input_file_path = get_abs_file_path(input_dir, input_fn)
    data = sc.textFile(input_file_path)

    # take out header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # need to convert list of strings to key value pairs
    #[[u1, mi], ..]
    user_pairs = data.map(lambda x: [int(i) for i in x.split(",")])

    # sorted makes sure that i,j == j,i
    # group pairs [(ui, [sortedmovies_ij]]
    grouped_users = user_pairs.groupByKey().map(lambda x: (x[0], sorted(x[1])))

    # need movies as keys and to expand the combinations of movies watched
    # combinations should keep sorted order
    # need (m1, m2), (m1, m4) vice [[u1, (m1,m2,m4)], ...] 
    movie_pairs = grouped_users.flatMap(lambda x: list(itertools.combinations(x[1], 2)))

    # Count pairs
    stripe_count_dict = movie_pairs.countByValue()

    # Output results
    output_dir = "output"
    output_fn = "tiny_run.csv"
    output_path = get_abs_file_path(output_dir, output_fn)
    save_default_dict(stripe_count_dict, output_path)
    print("saving cooccurrence counts")

if __name__ == "__main__":
    main()
