# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
#
# This generates all the pairs of products.
# Usage:
# python3 generate_pairs.py > pairs.txt

vector_file = "filtered_clothes_vectors.txt"

pairs = []
with open(vector_file) as f:
	lines = f.readlines()
	for i, line_a in enumerate(lines):
		product_a = line_a[1:11]
		for line_b in lines[i+1:]:
			product_b = line_b[1:11]
			print(product_a, product_b)

