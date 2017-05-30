# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
#
# This generates all the necessary pairs for the top 200
# products, used by interfad.py
# Usage:
# python3 generate_top_200_pairs.py > pairs_200.txt

top_200_file = "top_200_normed.txt"

with open(top_200_file) as g:
	lines_200 = g.readlines()

vector_file = "filtered_clothes_vectors.txt"

pairs = []
with open(vector_file) as f:
	lines_all = f.readlines()
	count = 0
	for line_a in lines_200:
		product_a = line_a.split(',')[1].strip()

		for line_b in lines_all:

			product_b = line_b[1:11]
			
			if product_a != product_b:
				
				print(product_a, product_b)

