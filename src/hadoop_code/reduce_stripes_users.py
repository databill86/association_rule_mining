#! /usr/bin/env python
import sys
import ast
from collections import defaultdict

# dictionary of int dictionaries that initialize with zero, does not pickle
# does hadoop need to pickle for multiprocessing?
# dict_of_dicts[movie_i][movie_j] = number of cooccurrences
stripes_dict = defaultdict(lambda: defaultdict(int))

# key = (movie_i, movie_j), value = cooccurrences
# movies_reviewed = defaultdict(int)
current_user = None
last_user = None
movies_reviewed = []
reduce_memory = True

# aggregated all the movies reviewed by user u
# now emit movie reviewed pairs
# pairs should be sorted so pair(a,b) is never (b,a)
def add_movies_to_dict(reviewed_movies, movies_dict):
    sorted_movies = sorted(reviewed_movies)
    for i in range(0, len(sorted_movies)):
        id_i = sorted_movies[i]
        j_start = i + 1
        for j in range(j_start, len(sorted_movies)):
            id_j = sorted_movies[j]
            movies_dict[id_i][id_j] += 1
    return movies_dict

def process_input(user, movies):
    usr = int(user)
    # lists are emited as string eg '[1,2,3]'
    m_ids = ast.literal_eval(movies)
    return usr, m_ids

def emit_stripe(stripes_dict):
    for movie_i in stripes_dict.keys():
        print("{}\t{}".format(movie_i, list(stripes_dict[movie_i].items())))
    
#input from STDIN
# userid \t movieids [m1, m2, ..]
for line in sys.stdin:
    verbose = False
    if verbose:
        print("line")
        print(line)
    # need to see how to check if list was sent or not
    # can user list(itertools.chain.from_iterable(some_list))
    user, movies = line.split('\t', 1)
#    assert( 1, [1,2] == process_input(1, '[1,2]'))
    try:
        current_user, movie_ids = process_input(user, movies)
        if verbose:
            print("try successful")
            print("userid:{}\t movieids:{}".format(current_user, movie_ids))

    except ValueError:
        if verbose:
            print("value error")
        continue

    # normally check for boundaries but doesnt help here, user_id vice movie id
    # this might be causing a problem because everthing is being held in memory
    if last_user and (current_user != last_user):
        stripes_dict = add_movies_to_dict(movies_reviewed, stripes_dict)
        if verbose:
            print("new user")

        # saves memory but not efficient because each stripe is going to only
        # have a count of 1
        if reduce_memory:
            emit_stripe(stripes_dict)
            stripes_dict = defaultdict(lambda: defaultdict(int))

        movies_reviewed = [movie_id for movie_id in movie_ids]

    # previous user
    else:
        if verbose:
            print("previous_user")
        for movie_id in movie_ids:
            movies_reviewed.append(movie_id)

    last_user = current_user

# last user
stripes_dict = add_movies_to_dict(movies_reviewed, stripes_dict)
emit_stripe(stripes_dict)
    
