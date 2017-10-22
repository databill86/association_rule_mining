import os
import csv
import sys

#verbose = False
#r_fn = sys.argv[1] 
#w_fn = "../{}_processed.csv".format(r_fn.split(".csv")[0])
#if verbose:
#    print(r_fn)
#    print(w_fn)

base_dir = "/home/ubuntu/hw2/data/"
r_fn = os.path.join(base_dir, "ratings.csv")
w_fn = os.path.join(base_dir, "ratings_processed.csv")
with open(w_fn, 'w') as output:
    wr = csv.writer(output)
    with open(r_fn, 'rt') as input:
        header = True
        for row in csv.reader(input):
            if verbose:
                print(row)
            if header:
                wr.writerow(row[:2])
                header = False
            else:
                rating = float(row[2])
                if rating >= 4:
                    wr.writerow(row[:2])

