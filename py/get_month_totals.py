# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
#
# This counts up the month totals, but they are now hard-coded into bin.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import time

class GetTotals(MRJob):

    def mapper(self, _, line):
       	rating_tuple = line.split(',')
        year = str(datetime.datetime.fromtimestamp(int(rating_tuple[3])).year)
        month = str(datetime.datetime.fromtimestamp(int(rating_tuple[3])).month)
        if year >= '2004':
            if len(month) == 1:
                month = '0' + month    
            yield (year + '-' + month), 1

    def combiner(self, month, ratings):
        yield month, sum(ratings)

    def reducer(self, month, ratings):
        yield None, sum(ratings) 
    
if __name__ == '__main__':
    GetTotals.run()