import sys

for line in sys.stdin:
    fields = line.split()
    user_id = fields[0]
    movie_id = list(fields[1])
    print('{}\t{}'.format(user_id, movie_id))
