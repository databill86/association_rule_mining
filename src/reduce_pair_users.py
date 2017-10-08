#! /usr/bin/env python
import sys
import ast
# import itertools

movies_reviewed = []
#input from STDIN
# userid \t movieids
for line in sys.stdin:
    # need to see how to check if list was sent or not
    # can user list(itertools.chain.from_iterable(some_list))
    user, movies = line.split('\t', 1)
    try:
        # single value
        movie_id = int(movies)
        movies_reviewed.append(movie_id)

    # need additional check if working with combiners?
    except ValueError:
        # lists are emited as string eg '[1,2,3]'
        try:
            movie_ids = ast.literal_eval(movies)
            for movie_id in movie_ids:
                movies_reviewed.append(movie_id)
        # other issue
        except:
            continue

print(movies_reviewed)
# aggregated all the movies reviewed by user u
# now emit movie reviewed pairs
# pairs should be sorted so pair(a,b) is never (b,a)
movies_reviewed = sorted(movies_reviewed)

for i in range(0, len(movies_reviewed)):
    id_i = movies_reviewed[i]
    j_start = id_i + 1
    for j in range(j_start, len(movies_reviewed)):
        id_j = movies_reviewed[j]
        print("({},{})\t{}".format(id_i, id_j, [1]))
