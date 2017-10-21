from pyspark import sparkContext, SparkConf
import itertools
from collections import defaultdict

def save_default_dict(dict_file, fn):
    with open(fn, 'wb') as output:
        wr = csv.writer(output)
        wr.writerow(["m1_m2","counts"])
        for k, v in dict_file.iteritems():
            wr.writerow([k,v])

def main():
    # initialize spark
    conf = SparkConf().setMaster("local").setAppName("test_spark.py")
    sc = SparkContext(conf = conf)

    # read in file
    fn = "hw2/test_input/ratings_tiny_processed.csv"
    data = sc.textFile(fn)

    # int, dont count header
    n_reviews = data.count() - 1 

    # take out header
    header = data.first()
    data = data.filter(lambda x: x != header)

    # need to convert list of strings to key value pairs
    #[[u1, mi], ..]
    user_pairs_rdd = data.map(lambda x: [int(i) for i in x.split(",")]).persist()

    # default dict of count times each movie got a high review
    # {keyi: n_i, ....}
    movie_counts_dict = user_pairs_rdd.map(lambda x: x[1]).countByValue()

    # sorted makes sure that i,j == j,i
    # group pairs [(ui, [sortedmovies_ij]), ...]
    grouped_users = user_pairs_rdd.groupByKey().map(lambda x: (x[0], sorted(x[1])))

    # need movies as keys and to expand the combinations of movies watched
    # combinations should keep sorted order
    # need (m1, m2), (m1, m4) vice [[u1, (m1,m2,m4)], ...] 
    movie_pairs = grouped_users.flatMap(lambda x: list(itertools.combinations(x[1], 2)))

    # Count pairs, default_dict type
    stripe_count_dict = movie_pairs.countByValue()

    # P(A|B) := P(A&B)/P(B) = |A&B|/|B| <- magnitudes or counts
    # first creat a list of keys, [(A,B), ..]
    keys_rdd = sc.parallelize(stripe_count_dict.keys())
    # Perform calc
    conditional_probs_l = keys_rdd.map(lambda k: float(stripe_count_dict[k]) /
                                       movie_counts_dict[k[1]])
    

#    for k, n_intersect in s_counts.iteritems():
#        i,j = k
#        n_b = float(movie_counts_dict[j])
#        print("P{}\t|{}&{}|:{}\t|{}|:{}".format(k, i, j, n_itersect, n_b))


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

