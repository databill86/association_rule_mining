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

movie_counts = grouped_movies.flatMap(dict_count).groupByKey()

# there should probably be a reduce action somehwere here instead of doing what i did below:wq
# am i done here?

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
# dict of dicts so I am doing a two level for loop, thj0 is the second dict index, j1 is the value ati and j

    return [((i, j[0], j[1]) for i, j_dict in stripe_dict.items() for j in j_dict.items()]
