# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
# ALGORITHM 1: INTER-FAD
#
#   This algorithm will pairwise compare all the products to find products with
# the SAME SHAPE, SAME MAGNITUDE, different time of popularity curves.

from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import sqlite3
import os
from ast import literal_eval

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_FILENAME = os.path.join(DATA_DIR, 'products.db')

class AnalyzePairs(MRJob):

    def mapper_init(self):
        self.conn = sqlite3.connect(DATABASE_FILENAME)
        self.cur = self.conn.cursor() 

    def mapper(self,  _, line):
        '''
        Checks the distance between every pair of vectors,
        and possible time shift
        '''
        pair = tuple(line.split(' '))

        # Fetches the normed vectors for our pair of products
        get_vec_query = 'SELECT vector_normed FROM products WHERE product_id = ? OR product_id = ?;'
        self.cur.execute(get_vec_query, pair)

        vecs = []
        for e in self.cur:
            vec = literal_eval(e[0])
            vecs.append(vec)

        # Performs the iterative time shift
        if len(vecs) == 2: # sometimes missing results, we skip those
            distances = []
            for shift in range(-60, 60):
                dist = 0
                for i in range(0, 91 - abs(shift)):
                    dist += abs(vecs[0][i] - vecs[1][i + shift])
                    dist /= 91 - abs(shift)
                distances.append(dist)

            # Smoothing algorithm for the resulting popularity curve
            smoothed = []
            k=2
            for i in range(k, len(distances) - k):  
                total = 0
                for j in range(-k, k):
                    total += distances[i + j]
                smoothed.append(total/(2*k+1))
            yield pair[0], (pair[1], min(smoothed))

    def mapper_final(self):
        self.conn.close()
    
    def reducer_init(self):
        self.conn = sqlite3.connect(DATABASE_FILENAME)
        self.cur = self.conn.cursor()

    def reducer(self, a_id, b_id_dist ):     
        vec_dist = list(b_id_dist)
        min_vecs = vec_dist[0:5]
        
        # Calculates the Least Distance Vector
        for v in vec_dist:
            if v[1] < max(min_vecs, key=lambda x: x[1])[1]:
                min_vecs.append(v)
                rm = max(min_vecs, key=lambda x: x[1])
                min_vecs.remove(rm)
        
        # Gets titles to display instead of product IDs
        get_title_query =  'SELECT title FROM products WHERE product_id = ?'
        self.cur.execute(get_title_query, (a_id, ))
        a_title = self.cur.fetchall()[0]

        best_matches = []
        for mv in min_vecs:
            self.cur.execute(get_title_query, (mv[0], ))
            b = self.cur.fetchall()[0]
            best_matches.append(b)

        yield a_title, best_matches

    def reducer_final(self):
        self.conn.close()


if __name__ == '__main__':
    AnalyzePairs.run()
