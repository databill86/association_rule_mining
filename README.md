Part 0 -- Download and Clean Files
---------------
Download file at grouplens
wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
extract to programming_dirctory/data
run preprocessing_make_reviews_binary.py

Part 1 -- Spark and Hadoop
---------------
1.a ) Compute frequency of co-occurrences for movies that recieve "high" ranking from users (e.g. movie a and b recieve high rankings from user 1, 6, and 10)
 - High ranking is 4 or 5 stars
 - Do first with pairs methodology
 - Second with stripes methodology

1.b) Compare speed of job with various dataset sizes and graph

1.c) Find the 20 most frequent movie pairs (by name)

1.d) Compare runtimes of Spark and Hadoop

Part 2 -- Spark
---------------
2.a) Compute P(movieB|movieA) with stripe methodology, conditional probability that movieB will be highly rated given movieA

2.b) List items where P(B|A) > 0.8

2.c) Graph time vs dataset size

Part 3 -- Spark
---------------
3.a) Compute lift between movieA and movieB
 - Lift(AB) = P(AB)/(P(A) * P(B)) = P(A|B)/P(A)

3.b) Plot time vs dataset size

3.c) Output Lift(AB) > 1.6, explain meaning 
