#!/usr/bin/python
import os
import csv
import sys

#r_fn = sys.argv[1] 
#w_fn = "../{}_processed.csv".format(r_fn.split(".csv")[0])
#if verbose:
#    print(r_fn)
#    print(w_fn)


verbose = False
def get_abs_file_path(file_dir, fn):
    cur_dir = os.path.abspath(os.curdir)
    return os.path.normpath(os.path.join(cur_dir, "..", file_dir, fn))

r_fn = get_abs_file_path("data", "ratings.csv")
w_fn = get_abs_file_path("data", "ratings_processed.csv")
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

