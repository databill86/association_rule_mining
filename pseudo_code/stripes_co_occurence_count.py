class Mapper:
    method Map_Users(userid u, movieid m, rating i):
    Emit(userid u, m)

class Reducer:
    method Reduce_Users(userid u, movieids[m1, m2, ...]):
    Movies_reviewed = new Array
    for all movieid m in movieids[m1, m2, ...] do:
        Append(Movies_reviewed, m)
    for all movieid i in Movies_reviewed do:
        for all movieid j not i in Movies_reviewed do:
            Emit(pair(movieid i, movieid j), 1)

class Reducer:
    method Reduce_Cooccurrence_Count(pair p, counts [c1, c2, ...])
    cooccurence_sum = 0
    for all count c in counts [c1, c2, ...] do:
        cooccurence_sum = cooccurence_sum + c
    Emit(pair p, cooccurrence_sum)
    method Map(user u, movies_reviewed m)
    for all reviews i in movies_reviewed m do:
        for all user v not user u do:
            if user v has review i do:
                for all review j not review i in movies_reviewed n do:
                    Emit(pair(i, j), 1)

class Reducer
    method Reduce(pair p, counts [c1, c2, ...])
    cooccurence_sum = 0
    for all count c in counts [c1, c2, ...] do:
        cooccurence_sum = cooccurence_sum + c
    Emit(pair p, cooccurrence_sum)
