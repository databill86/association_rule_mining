#! /usr/bin/env python
import sys
import ast
# import itertools

movies_reviewed = []
current_user = None
last_user = None

def emit_movies_reviewed(reviewed_movies):
    sorted_movies = sorted(reviewed_movies)
    for i in range(0, len(sorted_movies)):
        id_i = sorted_movies[i]
        j_start = id_i + 1
        for j in range(j_start, len(sorted_movies)):
            id_j = sorted_movies[j]
            print("({},{})\t{}".format(id_i, id_j, [1]))

#input from STDIN
# userid \t movieids
for line in sys.stdin:
    # need to see how to check if list was sent or not
    # can user list(itertools.chain.from_iterable(some_list))
    user, movies = line.split('\t', 1)
    try:
        # single value
        movie_ids = [int(movies)]
        current_user = int(user)

    # need additional check if working with combiners?
    except ValueError:
        # lists are emited as string eg '[1,2,3]'
        try:
            movie_ids = ast.literal_eval(movies)

        # other issue
        except:
            continue

    if current_user and (current_user != last_user):
        emit_movies_reviewed(movies_reviewed)
        movies_reviewed = []
        for movie_id in movie_ids:
            movies_reviewed.append(movie_id)

    else:
        for movie_id in movie_ids:
            movies_reviewed.append(movie_id)
    last_user = current_user

# aggregated all the movies reviewed by user u
# now emit movie reviewed pairs
# pairs should be sorted so pair(a,b) is never (b,a)
