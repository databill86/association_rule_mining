import os
import csv
import pandas as pd
import numpy as np

r_fn = "data/ratings.csv"
w_fn = "data/ratings_processed.csv"
with open(w_fn, 'w') as output:
    wr = csv.writer(output)
    with open(r_fn, 'rt') as input:
        header = True
        for row in csv.reader(input):
            if header:
                wr.writerow(row[:2])
                header = False
            else:
                rating = float(row[2])
                if rating >= 4:
                    wr.writerow(row[:2])

