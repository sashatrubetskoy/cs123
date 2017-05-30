# Vinay
# bin.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import time

# TOTAL number of product ratings for each month 2007-14
TOTALS = [8240, 2799, 3786, 1829, 4229, 
 2652, 3165, 3629, 3750, 3757, 3975, 6165, 7913, 5877, 
 4657, 5037, 5075, 5259, 5356, 5870, 6621, 7208, 7565, 
 9075, 13307, 8851, 8382, 8096, 8876, 7804, 5880, 7290, 
 10004, 9570, 10012, 12312, 14952, 13580, 12013, 10189, 
 10523, 10568, 12822, 13819, 15824, 17438, 19000, 24452, 
 27878, 20638, 22066, 19930, 21326, 23352, 24710, 27322, 
 30069, 35510, 34652, 49148, 51994, 39179, 39088, 36628, 
 39027, 39294, 44073, 45430, 52474, 91564, 97153, 206355, 210294, 
 161853, 163085, 160883, 168205, 159503, 166321, 179206, 
 169954, 214596, 212615, 321830, 324882, 259329, 310616, 
 262257, 262212, 272744, 223266]

class BinByMonth(MRJob):
    
    def mapper_bin_month(self, _, line):
        '''
        Reads in a ratings.csv

        For each rating, identifies product and date, and ensures
        the date is after 2007.

        Outputs a tuple of (product, date), and count of 1
        '''
        rating_tuple = line.split(',')
        product = rating_tuple[1]
        year = str(datetime.datetime.fromtimestamp(int(rating_tuple[3])).year)
        month = str(datetime.datetime.fromtimestamp(int(rating_tuple[3])).month)
        if year >= '2007':
            if len(month) == 1:
                month = '0' + month    
            yield ((product, year + '-' + month), 1)

    def combiner_bin_month(self, month, ratings):
        yield month, sum(ratings)

    def reducer_bin_month(self, month, ratings):
        yield month, sum(ratings)

    def mapper_vectorize_product(self, month, ratings):
        '''
        Generates tuples of product id and the date
        '''
        product_id = month[0]
        month_rating = (month[1], ratings)
        yield product_id, month_rating

    def reducer_vectorize_product(self, product_id, month_rating):
        '''
        Generates a rating vector.

        Iterates through year-month combination, and 
        tallies the normalized count of ratings for each product.

        The commented-out code is run to generate normed vectors.
        This was run twice.
        '''
        rating_vector = list(month_rating)
        for y in range(2007, 2015):
            for m in range(1, 13):
                if y == 2014 and m > 7:
                    break
                if m < 10:
                    date = str(y) + '-0' + str(m)
                else: 
                    date = str(y) + '-' + str(m)
                if date not in [x[0] for x in rating_vector]:
                    rating_vector.append([date, 0])
        
        rating_vector = sorted(rating_vector)
        np_vector = [x[1] for x in rating_vector]
        
        # norm = []
        norm_by_mag = []
        sum_np = sum(np_vector)
        for vec in np_vector:
            norm_by_mag.append(vec/sum_np)
        # for vec, tot in zip(np_vector, TOTALS):
            # norm.append(vec/tot)
        if sum(np_vector) > 50:
            # yield product_id, norm
            yield product_id, norm_by_mag


    def steps(self):
        return [
          MRStep(mapper=self.mapper_bin_month,
                 combiner=self.combiner_bin_month,
                 reducer=self.reducer_bin_month),
          MRStep(mapper = self.mapper_vectorize_product,
                reducer=self.reducer_vectorize_product)
        ]

    
if __name__ == '__main__':
    t0 = time.time()
    BinByMonth.run()
