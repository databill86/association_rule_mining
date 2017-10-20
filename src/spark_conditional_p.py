from pyspark import sparkContext, SparkConf
import itertools
from collections import defaultdict

def save_default_dict(dict_file, fn):
    with open(fn, 'wb') as output:
        w = csv.writer(output)
        w.writerow(["m1_m2","counts"])
        for k,v in dict_file.iteritems():
            w.writerow([k,v])

def main():
    # initialize spark
    conf = SparkConf().setMaster("local").setAppName("test_spark.py")
    sc = SparkContext(conf = conf)

    # read in file
    fn = "hw2/test_input/ratings_tiny_processed.csv"
    data = sc.textFile(fn)

    # take out header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # need to convert list of strings to key value pairs
    #[[u1, mi], ..]
    user_pairs = data.map(lambda x: [int(i) for i in x.split(",")])

    # int
    n_reviews = user_pairs.count()

    # sorted makes sure that i,j == j,i
    # group pairs [(ui, [sortedmovies_ij]]
    grouped_users_rdd = user_pairs.groupByKey().map(lambda x: (x[0], sorted(x[1]))).collect()

    # creates list of all movies, the creates a dictionary
    movie_counts_dict = grouped_users.flatMap(lambda x: x[1]).countByValue()

    # need movies as keys and to expand the combinations of movies watched
    # combinations should keep sorted order
    # need (m1, m2), (m1, m4) vice [[u1, (m1,m2,m4)], ...] 
    movie_pairs = grouped_users_rdd.flatMap(lambda x: list(itertools.combinations(x[1], 2)))

    # Count pairs
    stripe_count_dict = movie_pairs.countByValue()

if __name__ == "__main__":
    main()

# ---- not needed ---- #
# add ones to pairs to reduce
# added sort just in case probably not necessary, its assocative and communitive
# [((m1, m2), 1), ... ]
# movie_pairs_ones = movie_pairs.map(lambda x: (sorted(x), 1))

# [((m1,m2), count), ... ]
# x and y are values vij and vij+1
# stripes = movie_pairs_ones.reduceByKey(lambda x,y: x+y)

