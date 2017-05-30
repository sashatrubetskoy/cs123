# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
# ALGORITHM 2: INTRA-FAD
#
#   This algorithm will pairwise compare all the products to find products with
# the SAME SHAPE, SAME TIME, different magnitude of popularity curves.

import sqlite3
import os

from mrjob.job import MRJob
from mrjob.step import MRStep
from ast import literal_eval

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_FILENAME = os.path.join(DATA_DIR, 'products.db')
THRESHOLD = 0.2

class FindSameShapeTime(MRJob):

    def mapper_init(self):
        self.conn = sqlite3.connect(DATABASE_FILENAME)
        self.cur = self.conn.cursor()

    def mapper(self, _, line):
        '''
        Checks the distance between every pair of normalized vectors, WIHTOUT
        performing time shift
        '''
        pair = tuple(line.split(' '))

        get_vec_query = 'SELECT vector_normed FROM products WHERE product_id = ? OR product_id = ?'

        self.cur.execute(get_vec_query, pair)
        
        vecs = []
        for e in self.cur:
            vec = literal_eval(e[0])
            vecs.append(vec)

        if len(vecs) == 2:
            distance = 0
            for month_a, month_b in zip(vecs[0], vecs[1]):
                distance += abs(float(month_a) - float(month_b))

            if distance <= THRESHOLD:
                yield (pair, distance), 1

    def mapper_final(self):
        self.conn.close()

    def combiner(self, pd, _):
        pair, distance = pd
        yield pair[0], pair[1]
        yield pair[1], pair[0]

    def reducer_init(self):
        self.conn = sqlite3.connect(DATABASE_FILENAME)
        self.cur = self.conn.cursor()        

    def reducer(self, leader, neighbors):

        get_vec_query = 'SELECT title FROM products WHERE product_id = ?'
        
        self.cur.execute(get_vec_query, (leader,))
        leader_title = self.cur.fetchall()

        neibs = []
        for n in neighbors:
            self.cur.execute(get_vec_query, (n,))
            lmao = self.cur.fetchall()
            neibs.append(lmao[0][0])

        yield leader_title[0][0], neibs

    def reducer_final(self):
        self.conn.close()
        
if __name__ == '__main__':
    FindSameShapeTime.run()