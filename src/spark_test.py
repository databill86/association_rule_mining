from pyspark import sparkContext, SparkConf
import itertools
from collections import defaultdict

conf = SparkConf().setMaster("local").setAppName("test_spark.py")
sc = SparkContext(conf = conf)

data = sc.textFile("hw2/test_input/ratings_tiny_processed.csv")
header = data.first()

# take out header
data = data.filter(lambda x: x != header)


# need to convert list of strings to key value pairs
user_pairs = data.map(lambda x: x.split(",")

# grouped pairs [(u, [sortedmovies]_]
grouped_users = user_pairs.groupByKey().map(lambda x: (x[0], sorted(x[1]))

# need movies as keys and to expand the combinations of movies watched
# need (m1, m2), (m1, m4) vice [u1, (m1,m2,m4)] 
movie_pairs = grouped_users.flatMap(lambda x: list(itertools.combinations(x[1], 2)))

# group movies (m1, (m2, m4))
grouped_movies = movie_pairs.groupByKey().map(lambda x: (x[0], list(x[1])))

# now i need to count

def dict_counts(movie_stripes):
    # dict of dicts that initializes with a zero count
    stripe_dict = defaultdict(lambda: defaultdict(int))
    mv_i = movie_stripes[0]
    movie_list = (mv for mv in movie_stripes[1])
    for mv_j in movie_list:
        stripe_dict[mv_i][mv_j] += 1 

    key_pair_list = []
    # this doesnt work
    # extend keeps the list flat, the first items call returns a list of turls with inner dicts
    # the vaues of the 
    [key_pair_list.extend(stripe_dict.items()[i][1].items()) for i in xrange(0,len(s_dict.items()))]
 [l.extend((s_dict.items()[i][0], s_dict.items()[i][1].items())) for i in xrange(0, len(s_dict.items()))]   


    return
