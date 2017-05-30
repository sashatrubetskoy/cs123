# Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
# CS 123 Final Project
# ALGORITHM 2: INTRA-FAD
#
#   This program removes redundancies in the generated neighbors list.

from ast import literal_eval

neighbors_list = []
TEST_LINE = '"12312312"	["B234", "FWEFWEF", "WEF"]'

# Read the file into memory
with open('neighbors_list.txt') as f:
	for line in f.readlines():
		product_id, neighbors = tuple(line.split('\t'))
		neighbors = literal_eval(neighbors)
		neighbors_list.append((product_id, neighbors))

# Sort the list by how many neighbors
neighbors_list = sorted(neighbors_list, key = lambda x: -len(x[1]))

# Remove redundants
skips = []
clusters = []

for product in neighbors_list:
	lead_title, neighbors = product
	lead_title = lead_title[1:-1]
	cluster = []
	if lead_title not in skips:
		cluster.append(lead_title)
		for n in neighbors:
			cluster.append(n)
			skips.append(n)
		clusters.append(cluster)

# Print out list
for c in clusters:
	print(c)
