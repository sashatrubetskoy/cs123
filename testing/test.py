import sqlite3
import os
from ast import literal_eval

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_FILENAME = os.path.join(DATA_DIR, 'products.db')

conn = sqlite3.connect(DATABASE_FILENAME)
cur = conn.cursor()

with open('pairs_small.txt') as f:
    for line in f.readlines():
        pair = tuple([x.strip() for x in line.split(' ')])
        print(pair)

        get_vec_query = 'SELECT vector_normed FROM products WHERE product_id = ? OR product_id = ?;'

        cur.execute(get_vec_query, pair)

        vecs = []
        for e in cur:
            print(e)
            vec = literal_eval(e[0])
            vecs.append(vec) 

        distance = 0
        #print(vecs)
        for month_a, month_b in zip(vecs[0], vecs[1]):
            distance += abs(float(month_a) - float(month_b))